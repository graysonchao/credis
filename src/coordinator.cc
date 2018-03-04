#include <iostream>
#include <thread>
#include <boost/optional/optional.hpp>
#include "glog/logging.h"
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
          auto status = std::unique_ptr<grpc::Status>(new grpc::Status());
          auto initial_state = etcd_->Range(
              chain_prefix,
              /*range_end=*/etcd::util::RangePrefix(chain_prefix),
              /*revision=*/-1,
              *status
          );
          if (status->ok()) {
            ListenForChanges(chain_id, *initial_state);
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

std::unique_ptr<SyncWatchStream> Coordinator::WatchFromRevision(
    std::string chain_id, int start_revision
) {
  std::unique_ptr<SyncWatchStream>
      changes;
  WatchResponse wr;
  LOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  backoff::ExponentialBackoff(
      [this, &wr, &start_revision, &changes, chain_id] ()->bool {
          changes = std::move(etcd_->WatchCreate(
              kKeyPrefix + chain_id,
              /*range_end=*/etcd::util::RangePrefix(kKeyPrefix + chain_id),
              /*start_revision=*/start_revision,
              /*progress_notify=*/true,
              /*filters=*/{},
              /*prev_kv=*/false
          ));
          bool read_success = changes->Read(&wr);
          if (!read_success || wr.canceled()) {
            LOG(INFO) << "Failed to create watch stream. Retrying...";
            return false;
          }
          return true;
      },
      // These are not well-justified numbers
      /*interval_ms=*/100,
      /*timeout_ms=*/30*1000,
      /*multiplier=*/1.75
  );
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
  if (initial_state.kvs_size() < 1) {
    LOG(INFO) << "chain " << chain_id << " not found. Is the chain ID correct?";
  }

  /* Cached ongoing state of the chain - will be updated as events come in, and
   * updated as writes are performed on etcd. To prevent staleness, this state is
   * rebuilt from what is stored in etcd every time this function is called
   * - e.g. it doesn't survive crashes. */
  std::map<std::string, std::string> key_values;
  for (auto &kv: initial_state.kvs()) {
    if (!IsSystemKey(kv.key())) {
      key_values[kv.key()] = kv.value();
      LOG(INFO) << kv.key() << ": " << kv.value();
    }
  }
  auto key_prefix_with_no_colon = kKeyPrefix.substr(0, kKeyPrefix.size()-1);
  Chain chain(key_prefix_with_no_colon, chain_id, key_values);
  int64_t start_revision = FlushChain(chain);
  auto changes = WatchFromRevision(chain_id, start_revision);
  LOG(INFO) << "Watching chain " << chain_id << " from revision " << start_revision;
  WatchResponse watch_buffer;
  std::thread cleanup;
  while (true) {
    // If the stream ends, try to reconnect. If we can't, ???
    if (!changes->Read(&watch_buffer)) {
      return ManageChain(chain_id);
    }
    // This is meant to help keep start_revision up to date, so happens periodically
    // at a rate decided by etcd based on etcd server load. We'll piggyback on it
    // to clean up dead config files, so that we'll end up causing less traffic
    // at times of high load.
    if (watch_buffer.events().empty()) {
      cleanup = std::thread(
          [this, chain_id, chain] () {
              DLOG(INFO) << "Cleaning dead config files.";
              auto channel = channel_;
              auto etcd = etcd::Client(channel);
              auto all_keys = etcd.Range(
                  kKeyPrefix + chain_id,
                  etcd::util::RangePrefix(kKeyPrefix + chain_id)
              );
              std::vector<int64_t> dead_ids;
              for (const auto &kv : all_keys->kvs()) {
                if (!IsSystemKey(kv.key())) {
                  auto m_key = MemberKey(kv.key());
                  if (m_key.type == "config") {
                    auto it = chain.members.find(m_key.member_id);
                    if (!it->second.heartbeat.is_initialized()) {
                      dead_ids.push_back(it->first);
                    }
                  }
                }
              }
              std::vector<Compare> comparisons;
              std::vector<RequestOp> success_ops;
              std::vector<RequestOp> failure_ops;
              for (auto id : dead_ids) {
                auto config_key = chain.ConfigPath(id);
                success_ops.push_back(*etcd::util::BuildDeleteRequest(config_key));
                DLOG(INFO) << "Cleaning " << config_key;
              }
              auto tx = etcd.Transaction(comparisons, success_ops, failure_ops);
              if (tx->succeeded()) {
                DLOG(INFO) << "Cleaning complete.";
              } else {
                DLOG(INFO) << "Cleaning failed!";
              }
          });
    }
    if (watch_buffer.header().revision() > start_revision) {
      LOG(INFO) << "Just got event #" << watch_buffer.header().revision()
                << "start_revision is " << start_revision;
      for (auto &event : watch_buffer.events()) {
        if (IsSystemKey(event.kv().key())) {
          LOG(INFO) << "Ignoring system key...";
          continue;
        }
        LOG(INFO) << "(Rev# " << event.kv().mod_revision() << ") "
                  << event.kv().key() << ": " << event.kv().value();
        auto key = MemberKey(event.kv().key());
        if (event.type() == Event_EventType_PUT && !event.kv().value().empty()) {
          if (key.type == "hb" && chain.members.find(key.member_id) == chain.members.end()) {
            start_revision = HandleNodeJoin(chain, key.member_id, event.kv().value());
          }
        } else if (event.type() == Event_EventType_DELETE) {
          if (key.type == "hb" && chain.members.find(key.member_id) != chain.members.end()) {
            start_revision = HandleHeartbeatExpired(chain, key.member_id);
          }
        } else {
          LOG(INFO) << "Ignoring strange key "
                    << event.kv().key() << " = " << event.kv().value();
        }
      }
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
  return FlushChain(chain);
}

int64_t Coordinator::HandleNodeJoin(
    Chain &chain,
    int64_t new_id,
    const std::string &new_hb_str
) {
  chain.AddMember(new_id, new_hb_str);
  LOG(INFO) << "New member joined: " << new_id;
  return FlushChain(chain);
}

/**
 * Flush the chain to etcd, returning the next revision AFTER the
 * update in which changes were flushed.
 * @param chain
 * @return
 */
int64_t Coordinator::FlushChain(chain::Chain &chain) {
  LOG(INFO) << "Flushing chain.";
  auto members_to_erase = chain.EnforceChain();
  int64_t flush_revision = -1;
  backoff::ExponentialBackoff(
      [this, &chain, &flush_revision, members_to_erase] ()->bool {
          std::vector<Compare> comparisons = {};
          std::vector<RequestOp> success_ops;
          std::vector<RequestOp> failure_ops = {};
          for (auto member: members_to_erase) {
            success_ops.push_back(
                *etcd::util::BuildDeleteRequest(chain.ConfigPath(member.id)));
            LOG(INFO) << "Deleting " << member.id;
          }
          for (auto &kv : chain.SerializedState()) {
            success_ops.push_back(*etcd::util::BuildPutRequest(kv.first, kv.second));
          }
          auto tx = etcd_->Transaction(comparisons, success_ops, failure_ops);
          if (tx->succeeded()) {
            LOG(INFO) << "Flushed chain configs for " << chain.id;
            flush_revision = tx->header().revision();
            return true;
          } else {
            LOG(INFO) << "Failed to flush chain configs for " << chain.id
                      << ". Retrying...";
            return false;
          }
      },
      // These are not well-justified numbers
      /*interval_ms=*/100,
      /*timeout_ms=*/30*1000,
      /*multiplier=*/1.75
  );
  return flush_revision + 1;
}

bool Coordinator::IsSystemKey(const std::string &key) {
  return key.find("_join") != std::string::npos ||
         key.find("_last_id") != std::string::npos;
}

