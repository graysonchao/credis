#include <iostream>
#include <thread>
#include "glog/logging.h"
#include "coordinator.h"

using namespace chain;
using json = nlohmann::json;

const std::string Coordinator::kKeyPrefix = "credis";
const Coordinator::Options kDefaultOptions {
    /*bool auto_add_new_members = */false
};

Coordinator::Coordinator(std::unique_ptr<etcd::ClientInterface> etcd,
                         Coordinator::Options options)
    : etcd_(std::move(etcd)), options_(options) {}
Coordinator::Coordinator(std::unique_ptr<etcd::ClientInterface> etcd)
    : etcd_(std::move(etcd)), options_(kDefaultOptions) {}

grpc::Status Coordinator::ManageChain(std::string chain_id) {
  auto chain_prefix = kKeyPrefix + ":" + chain_id;
  RangeResponse res;
  auto status = etcd::util::ExponentialBackoff(
      [this, chain_prefix, &res]() -> grpc::Status {
        RangeRequest req;
        req.set_key(chain_prefix);
        req.set_range_end(etcd::util::RangePrefix(chain_prefix));
        return etcd_->Range(req, &res);
      }
  );

  CHECK(status.ok()) << "Could not get chain state within timeout.";
  Chain chain(kKeyPrefix, chain_id);
  for (auto &kv: res.kvs()) {
    if (IsHeartbeatKey(kv.key())) {
      MemberKey key(kv.key());
      chain::Member new_member(key.member_id);
      new_member.heartbeat = MemberHeartbeat(kv.value());
      chain.AddMember(new_member);
    }
  }
  LOG(INFO) << "Managing " << chain_prefix;
  return ListenForChanges(&chain);
}

// Listen for changes to a chain and respond to failures by generating and
// writing a new intended config to etcd.
grpc::Status Coordinator::ListenForChanges(Chain *chain) {
  // We may be watching a chain that has already had a master that failed.
  // Fixes all dead nodes at once, in case some have died since the old master
  // failed.
  int64_t start_revision = WriteChain(chain);

  DLOG(INFO) << "Creating watch stream starting at revision " << start_revision;
  WatchRequest req;
  auto wcr = new WatchCreateRequest();
  wcr->set_key(kKeyPrefix + ":" + chain->id);
  wcr->set_range_end(etcd::util::RangePrefix(kKeyPrefix + ":" + chain->id));
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
        auto key = event.kv().key();
        auto raw_value = event.kv().value();
        bool just_created = (kv.mod_revision() == kv.create_revision());
        if (!IsSystemKey(key)) {
          DLOG(INFO) << "(Rev# " << kv.mod_revision() << ")" << kv.key() << ": "
                     << kv.value();
          switch(event.type()) {
            case Event_EventType_PUT:
              if (IsHeartbeatKey(key) && !raw_value.empty() && just_created) {
                chain::Member m(MemberKey(key).member_id);
                m.heartbeat = MemberHeartbeat(raw_value);
                chain->AddMember(m);
                chain_is_dirty = true;
              }
              break;
            case Event_EventType_DELETE:
              chain->RemoveMember(MemberKey(key).member_id);
              chain_is_dirty = true;
              break;
            default:
              DLOG(INFO) << "Ignoring strange key " << key;
          }
        }
      }

      if (chain_is_dirty) {
        DLOG(INFO) << "Canceled watch to flush chain." << start_revision;
        etcd_->WatchCancel(watch_res.watch_id());
        start_revision = WriteChain(chain);
        DLOG(INFO) << "Restarting watch at rev#" << start_revision;

        auto wcr = new WatchCreateRequest();
        wcr->set_key(kKeyPrefix + ":" + chain->id);
        wcr->set_range_end(etcd::util::RangePrefix(kKeyPrefix + ":" + chain->id));
        wcr->set_start_revision(start_revision);
        wcr->set_progress_notify(true);
        WatchRequest req;
        req.set_allocated_create_request(wcr);
        changes->Write(req);
      }
    }
  }

  // etcd should never call WritesDone() on its end. If we get here, then either
  // the pipe broke or someone canceled our watch.
  LOG(ERROR) << "Watch canceled."
             << " etcd gave this cancel reason: " << watch_res.cancel_reason();
  return grpc::Status::CANCELLED;
}

// Write intended configs based on the given chain to etcd. Removes dead
// members and deletes their intended configs as part of the transaction.
//
// Returns the revision # that corresponds to the transaction in etcd.
int64_t Coordinator::WriteChain(Chain* chain) {
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
  auto status = etcd::util::ExponentialBackoff(
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

bool Coordinator::IsSystemKey(const std::string& key) {
  return key.find("_join") != std::string::npos ||
         key.find("_last_id") != std::string::npos;
}

bool Coordinator::IsHeartbeatKey(const std::string& key) {
  const auto key_ending = "/" + chain::kKeyTypeHeartbeat;
  // key ends with key_ending
  return std::equal(key_ending.rbegin(), key_ending.rend(), key.rbegin());
}
