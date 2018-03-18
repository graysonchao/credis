#include <iostream>
#include <memory>
#include <thread>
#include <src/etcd/etcd_utils.h>
#include "glog/logging.h"
#include "etcd3/include/etcd3.h"
#include "etcd_master.h"
#include "src/chain.h"
#include "src/etcd/etcd_utils.h"

using namespace etcd3::pb;
using namespace chain;
using json = nlohmann::json;
using utils::ObservableRange;
using utils::ObserverFunc;


const EtcdMaster::Options kDefaultOptions {
    /*bool auto_add_new_members = */true
};

EtcdMaster::EtcdMaster() : options_(kDefaultOptions) {}
EtcdMaster::EtcdMaster(std::shared_ptr<grpc::Channel> channel,
                         EtcdMaster::Options options)
    : channel_(channel),
      options_(options) {}
EtcdMaster::EtcdMaster(std::shared_ptr<grpc::Channel> channel)
    : channel_(channel),
      options_ (kDefaultOptions) {}

void EtcdMaster::HandleStateChange(std::vector<etcd3::pb::Event> updates) {
  for (const etcd3::pb::Event& event : updates) {
    auto kv = event.kv();
    if (IsHeartbeatKey(kv.key()) && event.type() == Event_EventType_DELETE) {
      MemberKey member_key(kv.key());
      chain_.RemoveMember(member_key.id);
      LOG(INFO) << "responding to DEL " << event.kv().key();
      chain_is_dirty = true;
    }
  }
}

grpc::Status EtcdMaster::Connect(utils::EtcdURL url) {
  etcd_ = std::unique_ptr<etcd3::Client>(new etcd3::Client(channel_));
  etcd3::pb::RangeRequest config_range;
  config_range.set_key(url.chain_prefix);
  config_range.set_range_end(etcd3::util::RangePrefix(url.chain_prefix));
  ObservableRange configs(channel_, config_range, HandleStateChange);

  etcd3::pb::RangeResponse initial_state;
  auto status = configs.GetInitialState(&initial_state);

  chain_.reset(new Chain(url.chain_prefix));

  for (auto &kv: initial_state.kvs()) {
    if (IsHeartbeatKey(kv.key())) {
      chain.AddHeartbeat(chain::Heartbeat(kv));
    } else if (IsConfigKey(kv.key())) {
      chain.AddConfig(chain::Config(kv));
    }
  }

  return configs.Observe(false);
}

// Listen for changes to a chain and respond to failures by generating and
// writing a new intended config to etcd.
grpc::Status EtcdMaster::WatchChain(std::string chain_prefix) {
  DLOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  WatchRequest req;
  auto wcr = new WatchCreateRequest();
  wcr->set_key(chain.prefix);
  wcr->set_range_end(etcd3::util::RangePrefix(chain.prefix));
  wcr->set_start_revision(start_revision);
  wcr->set_progress_notify(true);
  req.set_allocated_create_request(wcr);
  auto changes = std::move(etcd_->MakeWatchStream(req));

  // Loop on change events.
  WatchResponse watch_res;
  while (changes->Read(&watch_res)) {
    if (watch_res.header().revision() > start_revision) {
      bool chain_is_dirty = false;
      for (auto& event : watch_res.events()) {
        auto kv = event.kv();
        auto key_str = event.kv().key();
        auto value_str = event.kv().value();
        if (IsHeartbeatKey(key_str)) {
          MemberKey member_key(key_str);
          switch(event.type()) {
            case Event_EventType_PUT: {
              if (!value_str.empty()) {
                chain::Member m(member_key.id);
                m.heartbeat = Heartbeat(value_str);
                if (m.heartbeat.config.role == chain::kRoleUninitialized
                    && !chain.HasMember(member_key.id)) {
                  LOG(INFO) << "respoding to PUT " << event.kv().key() << ": "
                            << event.kv().value();
                  LOG(INFO) << "chain has member id " << member_key.id
                            << chain.HasMember(member_key.id);
                  LOG(INFO) << member_key.id << "not in chain";
                  chain.AddMember(m);
                  chain_is_dirty = true;
                }
              }
              break;
            }
            case Event_EventType_DELETE: {
              chain.RemoveMember(member_key.id);
              LOG(INFO) << "responding to DEL " << event.kv().key();
              chain_is_dirty = true;
              break;
            }
            default:
              DLOG(INFO) << "Ignoring strange key " << key_str;
          }
        }
      }

      if (chain_is_dirty) {
        WriteChain(&chain);
      }
    }
  }

  // etcd should never call WritesDone() on its end. If we get here, then either
  // the pipe broke or someone canceled our watch.
  LOG(ERROR) << "Watch canceled."
             << " etcd gave this cancel reason: " << watch_res.cancel_reason();
  return grpc::Status::CANCELLED;
}

Chain EtcdMaster::ReadChain(const etcd3::Client& etcd,
                                   std::string prefix) {
  RangeResponse res;
  auto status = etcd3::util::ExponentialBackoff(
      [&etcd, &prefix, &res]() -> grpc::Status {
        RangeRequest req;
        req.set_key(prefix);
        req.set_range_end(etcd3::util::RangePrefix(prefix));
        return etcd.Range(req, &res);
      }
  );

  CHECK(status.ok()) << "Could not get chain state within timeout.";
  Chain chain(prefix);

  for (auto &kv: res.kvs()) {
    if (IsHeartbeatKey(kv.key())) {
      MemberKey key(kv.key());
      chain::Member new_member(key.id);
      new_member.heartbeat = Heartbeat(kv.value());
      chain.AddMember(new_member);
    }
  }
  return chain;
}

// Write intended configs based on the given chain to etcd. Removes dead
// members and deletes their intended configs as part of the transaction.
//
// Returns the revision # that corresponds to the transaction in etcd.
int64_t EtcdMaster::WriteChain(Chain* chain) {
  std::vector<int64_t> dead_member_ids;
  for (auto &it: chain->members) {
    if (!it.second.HasHeartbeat()) {
      dead_member_ids.push_back(it.first);
    }
  }
  for (auto member_id: dead_member_ids) {
    chain->RemoveMember(member_id);
  }

  // Do the update.
  int64_t flush_revision = -1;
  auto status = etcd3::util::ExponentialBackoff(
      [this, &chain, &flush_revision, &dead_member_ids]() -> grpc::Status {
        TxnRequest req;
        for (auto deleted_id: dead_member_ids) {
          auto config_key = chain->ConfigKey(deleted_id);
          auto dr = new DeleteRangeRequest();
          dr->set_key(config_key);
          req.add_success()->set_allocated_request_delete_range(dr);
        }

        // Delete configs for any members with lower ID than the head.
        // Since members atomically increment the base ID in order to join,
        // and etcd provides sequential consistency, nodes only ever have
        // children with a higher ID.
        auto delete_old_ids = new DeleteRangeRequest();
        delete_old_ids->set_key(chain->MemberDirectory(0));
        delete_old_ids->set_range_end(chain->MemberDirectory(chain->head_id));
        req.add_success()->set_allocated_request_delete_range(delete_old_ids);

        for (auto& kv : chain->SerializedState()) {
          auto pr = new PutRequest();
          pr->set_key(kv.first);
          pr->set_value(kv.second);
          req.add_success()->set_allocated_request_put(pr);
        }
        TxnResponse res;
        auto status = etcd_->Transaction(req, &res);
        if (res.succeeded()) {
          flush_revision = res.header().revision();
        }
        return status;
      }
  );
  CHECK(status.ok()) << "Failed to complete a write to etcd within timeout.";
  return flush_revision + 1;
}
