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
  std::unordered_map<std::string, std::string> key_values;
  for (auto &kv: initial_state.kvs()) {
    if (!IsSystemKey(kv.key())) {
      key_values[kv.key()] = kv.value();
    }
  }
  auto key_prefix_with_no_colon = kKeyPrefix.substr(0, kKeyPrefix.size()-1);
  Chain chain(key_prefix_with_no_colon, chain_id, key_values);

  WatchResponse wr;
  int64_t start_revision = initial_state.header().revision() + 1;
  LOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  bool watching = backoff::ExponentialBackoff(
      [this, &wr, &start_revision, chain_id] ()->bool {
          changes_ = etcd_->WatchCreate(
              kKeyPrefix + chain_id,
              /*range_end=*/etcd::util::RangePrefix(kKeyPrefix + chain_id),
              /*start_revision=*/start_revision,
              /*progress_notify=*/true,
              /*filters=*/{},
              /*prev_kv=*/false
          );
          bool read_success = changes_->Read(&wr);
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
  CHECK(watching) << "Could not create watch stream.";

  LOG(INFO) << "Watching chain " << chain_id << " from revision " << start_revision;
  while (true) {
    // If the stream ends, try to reconnect. If we can't, ???
    if (!changes_->Read(&wr)) {
      return ManageChain(chain_id);
    }
    for (auto &event : wr.events()) {
      if (IsSystemKey(event.kv().key())) {
        LOG(INFO) << "Ignoring system key...";
        continue;
      }

      LOG(INFO) << "(Rev# " << event.kv().mod_revision() << ") "
                << event.kv().key() << ": " << event.kv().value();
      auto key = MemberKey(event.kv().key());
      if (event.type() == Event_EventType_PUT && !event.kv().value().empty()) {
        if (key.type == "hb") {
          HandleNodeJoin(chain, key.member_id, event.kv().value());
        }
      } else if (event.type() == Event_EventType_DELETE) {
        if (key.type == "hb") {
          HandleHeartbeatExpired(chain, key.member_id);
        }
      } else {
        LOG(INFO) << "Ignoring strange key "
                  << event.kv().key() << " = " << event.kv().value();
      }
    }
    FlushChain(chain);
  }
}

/**
 * Handle the case that a heartbeat key disappeared, which means a
 * node failed.
 * @param chain_state
 * @param event
 */
void Coordinator::HandleHeartbeatExpired(Chain &chain, const std::string &failed_id) {
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
}

void Coordinator::HandleNodeJoin(
    Chain &chain,
    const std::string &new_id,
    const std::string &new_hb_str
) {
  if (chain.members.empty()) {
    chain.AddMember(new_id, new_hb_str);
    chain.SetRole(new_id, chain::kRoleSingleton);
    chain.SetPrev(new_id, chain::kNoMember);
    chain.SetNext(new_id, chain::kNoMember);
  } else if (chain.members.size() == 1) {
    auto new_head_id = chain.members.begin()->second.id;
    chain.AddMember(new_id, new_hb_str);
    chain.SetRole(new_head_id, chain::kRoleHead);
    chain.SetRole(new_id, kRoleTail);
    chain.SetPrev(chain.tail_id, chain.head_id);
    chain.SetNext(chain.head_id, chain.tail_id);
  } else {
    chain.AddMember(new_id, new_hb_str);
    chain.SetRole(chain.tail_id, kRoleMiddle);
    chain.SetNext(chain.tail_id, new_id);
    chain.SetPrev(new_id, chain.tail_id);
    chain.SetRole(new_id, kRoleTail);
  }
  LOG(INFO) << "New member joined: " << new_id;
}

void Coordinator::FlushChain(const chain::Chain &chain) {
  LOG(INFO) << "Flushing chain.";
  backoff::ExponentialBackoff(
      [this, chain] ()->bool {
          auto status = std::unique_ptr<grpc::Status>(new grpc::Status());
          std::vector<Compare> comparisons = {};
          std::vector<RequestOp> success_ops;
          std::vector<RequestOp> failure_ops = {};
          for (auto &kv : chain.SerializedState()) {
            success_ops.push_back(*etcd::util::BuildPutRequest(kv.first, kv.second));
          }
          etcd_->Transaction(comparisons, success_ops, failure_ops);
          if (status->ok()) {
            LOG(INFO) << "Flushed chain configs for " << chain.id;
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
}

bool Coordinator::IsSystemKey(const std::string &key) {
  return key.find("_join") != std::string::npos ||
         key.find("_last_id") != std::string::npos;
}

