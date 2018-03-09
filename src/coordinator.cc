#include <iostream>
#include <thread>
#include <boost/optional/optional.hpp>
#include <glog/logging.h>
#include "coordinator.h"

using namespace chain;
using json = nlohmann::json;

const std::string Coordinator::kKeyPrefix = "credis:";
const Coordinator::Options kDefaultOptions {
    /*bool auto_add_new_members = */false
};

Coordinator::Coordinator(std::unique_ptr<etcd::ClientInterface> etcd, Coordinator::Options options)
    : etcd_(std::move(etcd)), options_(options) {}

Coordinator::Coordinator(std::unique_ptr<etcd::ClientInterface> etcd)
    : etcd_(std::move(etcd)), options_(kDefaultOptions) {}

std::unique_ptr<grpc::Status> Coordinator::Connect(const std::string &address, int port) {
  backoff::ExponentialBackoff(
      [this, address, port] ()->bool {
          channel_ = grpc::CreateChannel(
              address + ":" + std::to_string(port),
              grpc::InsecureChannelCredentials()
          );
          if (channel_->WaitForConnected(std::chrono::seconds(2))) {
            etcd_ = std::unique_ptr<etcd::ClientInterface>(new etcd::Client(channel_));
            return true;
          };
          return false;
      },
      // These are not well-justified numbers
      /*interval_ms=*/100,
      /*timeout_ms=*/30*1000,
      /*multiplier=*/1.75
  );
}

void Coordinator::ManageChain(std::string chain_id) {
  backoff::ExponentialBackoff(
      [this, chain_id] ()->bool {
        std::string chain_prefix = kKeyPrefix + chain_id;
        RangeRequest req;
        req.set_key(chain_prefix);
        req.set_range_end(etcd::util::RangePrefix(chain_prefix));
        RangeResponse res;
        auto status = etcd_->Range(req, &res);
        if (status.ok()) {
          ListenForChanges(chain_id, res);
          LOG(INFO) << "Managing " << chain_prefix;
          return true;
        } else {
          LOG(INFO) << "Failed to get chain configs for "
                    << chain_prefix << ". Retrying...";
          return false;
        }
      },
      // These are not well-justified numbers
      /*interval_ms=*/100,
      /*timeout_ms=*/30*1000,
      /*multiplier=*/1.75
  );
}

etcd::WatchStreamPtr Coordinator::WatchFromRevision(std::string chain_id,
                                                    int start_revision) {
  etcd::WatchStreamPtr changes;
  WatchResponse wr;
  LOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  WatchRequest req;
  auto wcr = new WatchCreateRequest();
  wcr->set_key(kKeyPrefix + chain_id);
  wcr->set_range_end(etcd::util::RangePrefix(kKeyPrefix + chain_id));
  wcr->set_start_revision(start_revision);
  wcr->set_progress_notify(true);
  req.set_allocated_create_request(wcr);
  WatchResponse res;
  changes = etcd_->MakeWatchStream(req, &res);
  LOG(INFO) << wr.SerializeAsString();
  return changes;
  // backoff::ExponentialBackoff(
  //     [this, &wr, &start_revision, &changes, chain_id] ()->bool {
  //       WatchRequest req;
  //       auto wcr = new WatchCreateRequest();
  //       wcr->set_key(kKeyPrefix + chain_id);
  //       wcr->set_range_end(etcd::util::RangePrefix(kKeyPrefix + chain_id));
  //       wcr->set_start_revision(start_revision);
  //       wcr->set_progress_notify(true);
  //       req.set_allocated_create_request(wcr);
  //       WatchResponse res;
  //       changes = etcd_->MakeWatchStream(req, &res);
  //       LOG(INFO) << wr.compact_revision();
  //       LOG(INFO) << wr.SerializeAsString();
  //       if (wr.canceled()) {
  //         LOG(INFO) << "Failed to create watch stream. Retrying...";
  //         return false;
  //       }
  //       return true;
  //     },
  //     // These are not well-justified numbers
  //     /*interval_ms=*/100,
  //     /*timeout_ms=*/30*1000,
  //     /*multiplier=*/1.75
  // );
  return changes;
}

/**
 * Listen for changes to a chain and respond to failures by generating a new intended config.
 * The initial
 */
void Coordinator::ListenForChanges(
    const std::string& chain_id,
    const RangeResponse &initial_state
) {
  std::map<std::string, std::string> initial_chain_state;
  for (auto &kv: initial_state.kvs()) {
    if (!IsSystemKey(kv.key())) {
      initial_chain_state[kv.key()] = kv.value();
    }
  }
  auto key_prefix_with_no_colon = kKeyPrefix.substr(0, kKeyPrefix.size()-1);
  Chain chain(key_prefix_with_no_colon, chain_id, initial_chain_state);
  int64_t start_revision = WriteChain(chain);
  WatchResponse wr;
  LOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  WatchRequest req;
  auto wcr = new WatchCreateRequest();
  wcr->set_key(kKeyPrefix + chain_id);
  wcr->set_range_end(etcd::util::RangePrefix(kKeyPrefix + chain_id));
  wcr->set_start_revision(start_revision);
  wcr->set_progress_notify(true);
  req.set_allocated_create_request(wcr);
  WatchResponse watch_res;
  auto changes = std::move(etcd_->MakeWatchStream(req, &watch_res));
  while (true) {
    if (watch_res.header().revision() > start_revision) {
      for (auto &event : watch_res.events()) {
        if (!IsSystemKey(event.kv().key())) {
          DLOG(INFO) << "(Rev# " << event.kv().mod_revision() << ") "
                    << event.kv().key() << ": " << event.kv().value();
          auto key = MemberKey(event.kv().key());
          auto raw_value = event.kv().value();
          if (event.type() == Event_EventType_PUT && !raw_value.empty()) {
            if (key.type == "hb" && !chain.HasMember(key.member_id)) {
              start_revision = HandleNodeJoin(chain, key.member_id, raw_value);
            }
          } else if (event.type() == Event_EventType_DELETE) {
            if (key.type == "hb" && chain.HasMember(key.member_id)) {
              start_revision = HandleHeartbeatExpired(chain, key.member_id);
            }
          } else {
            DLOG(INFO) << "Ignoring strange key "
                       << event.kv().key() << " = " << event.kv().value();
          }
        }
      }
    }
    if (!changes->Read(&watch_res)) {
      DLOG(INFO) << "Failed to read from watch stream. Reconnecting...";
      return ManageChain(chain_id);
    }

  }
}

/**
 * Handle the case that a heartbeat key disappeared, which means a
 * node failed.
 * @param chain_state
 * @param event
 */
int64_t Coordinator::HandleHeartbeatExpired(Chain &chain, int64_t failed_id) {
  LOG(INFO) << "Heartbeat expired for member " << failed_id;
  auto failed_iter = chain.members.find(failed_id);
  if (failed_iter != chain.members.end()) {
    auto failed = failed_iter->second;
    if (failed.config->role == chain::kRoleHead) {
      chain.SetRole(failed.config->next, chain::kRoleHead);
      chain.SetPrev(failed.config->next, chain::kNoMember);
    } else if (failed.config->role == kRoleTail) {
      chain.SetRole(failed.config->prev, kRoleTail);
      chain.SetNext(failed.config->prev, chain::kNoMember);
    } else if (failed.config->role == kRoleMiddle) {
      chain.SetNext(failed.config->prev, failed.config->next);
      chain.SetPrev(failed.config->next, failed.config->prev);
    }
    chain.members.erase(failed_iter);
  } else {
    // Config file doesn't exist, so this node was never a member.
    LOG(INFO) << "Saw failed " << failed_id
              << " crash, but it hadn't yet joined. Is it flapping?";
  }
  return WriteChain(chain);
}

int64_t Coordinator::HandleNodeJoin(
    Chain &chain,
    int64_t new_id,
    const std::string &new_hb_str
) {
  chain.AddMember(new_id, new_hb_str);
  LOG(INFO) << "New member joined: " << new_id;
  return WriteChain(chain);
}

/**
 * Flush the chain to etcd, returning the next revision AFTER the
 * update in which changes were flushed.
 * @param chain
 * @return
 */
int64_t Coordinator::WriteChain(chain::Chain& chain) {
  LOG(INFO) << "Flushing chain.";
  auto members_to_erase = chain.EnforceChain();
  int64_t flush_revision = -1;
  backoff::ExponentialBackoff(
      [this, &chain, &flush_revision, members_to_erase]() -> bool {
        TxnRequest req;
        for (auto member: members_to_erase) {
          auto config_key = chain.ConfigPath(member.id);
          auto dr = new DeleteRangeRequest();
          dr->set_key(config_key);
          req.add_success()->set_allocated_request_delete_range(dr);
          DLOG(INFO) << "Deleting " << member.id;
        }
        for (auto& kv : chain.SerializedState()) {
          auto pr = new PutRequest();
          pr->set_key(kv.first);
          pr->set_value(kv.second);
          req.add_success()->set_allocated_request_put(pr);
        }
        TxnResponse res;
        auto status = etcd_->Transaction(req, &res);
        if (res.succeeded()) {
          LOG(INFO) << "Flushed chain configs for " << chain.id;
          flush_revision = res.header().revision();
          return true;
        } else {
          LOG(INFO) << "Failed to flush chain configs for " << chain.id
                    << ". Retrying...";
          return false;
        }
      },
      // These are not well-justified numbers
      /*interval_ms=*/100,
      /*timeout_ms=*/30 * 1000,
      /*multiplier=*/1.75
  );
  return flush_revision + 1;
}

bool Coordinator::IsSystemKey(const std::string& key) {
  return key.find("_join") != std::string::npos ||
         key.find("_last_id") != std::string::npos;
}

