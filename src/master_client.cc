#include <boost/optional/optional.hpp>
#include <chrono>
#include <string>
#include <thread>
#include <glog/logging.h>

#include "nlohmann/json.hpp"
#include "hiredis/hiredis.h"

#include "master_client.h"
#include "utils.h"
#include "chain.h"

using json = nlohmann::json;

const std::string MasterClient::kKeyPrefix = "credis:";
const std::string MasterClient::kNoAddress = "nil";
const int MasterClient::kHeartbeatIntervalSec = 5;
const int MasterClient::kHeartbeatTimeoutSec = 20;
const int kHeartbeatBackoffMultiplier = 2;

/**
 * Connect to the master.
 *
 * Assigns this member an ID that is unique among all chain members.
 * @param address
 * @param port
 * @return
 */
Status MasterClient::Connect(
    const std::string& address,
    const int port,
    const std::string& chain_id,
    const std::string& redis_addr,
    const int redis_port
) {
  channel_ = grpc::CreateChannel(address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
  etcd_ = std::unique_ptr<etcd::Client>(new etcd::Client(channel_));
  chain_id_ = chain_id;
  redis_addr_ = redis_addr;
  redis_port_ = redis_port;
  // Start a lease with etcd. Sending a KeepAlive for this lease implements a heartbeat.
  StartHeartbeat();
  // While holding a chain-wide lock, acquire the next available chain member ID and claim it for myself.
  AcquireID();
  // Monitor changes to my intended state.
  StartWatchingConfig();
  // Register myself as a member of the chain by putting my member ID as a key. The key disappears if heartbeat expires.
  RegisterMemberInfo();
  return Status::OK();
}

/**
 * Get the key for the type of watermark given by W.
 * @param w
 * @return
 */
const std::string MasterClient::WatermarkKey(std::string chain_id, Watermark w) const {
  std::string watermark_key = (w == MasterClient::Watermark::kSnCkpt) ? "_sn_ckpt" : "_sn_flushed";
  return kKeyPrefix + chain_id + "/" + watermark_key;
}

/**
 * Return the key containing the last assigned ID for the given chain.
 * @param chain_id
 * @return
 */
const std::string MasterClient::LastIDKey(std::string chain_id) const {
  return kKeyPrefix + chain_id + "/_last_id";
}

/**
 * Return the name of the lock that must be held in order to join the given chain.
 * @param chain_id
 * @return
 */
const std::string MasterClient::JoinLock(std::string chain_id) const {
  return kKeyPrefix + chain_id + "/_join";
}

/**
 * Return the heartbeat key for this member
 * @param chain_id
 * @return
 */
const std::string MasterClient::HeartbeatKey(std::string chain_id, int64_t member_id) const {
  return kKeyPrefix + chain_id + "/" + std::to_string(member_id) + "/hb";
}

/**
 * Return the config key for this member
 * @param chain_id
 * @return
 */
const std::string MasterClient::ConfigKey(std::string chain_id, int64_t member_id) const {
  return kKeyPrefix + chain_id + "/" + std::to_string(member_id) + "/config";
}

Status MasterClient::GetWatermark(Watermark w, int64_t* val) const {
  std::string key = WatermarkKey(chain_id_, w);
  RangeRequest req;
  RangeResponse res;
  req.set_key(key);
  req.set_range_end("");
  grpc::Status status = etcd_->Range(req, &res);

  if (res.kvs_size() > 0) {
    auto kvs = res.kvs();
    const KeyValue &result = kvs.Get(0);
    *val = std::stol(result.value());
    return Status::OK();
  } else {
    switch (w) {
      case Watermark::kSnCkpt:
        *val = kSnCkptInit;
            break;
      case Watermark::kSnFlushed:
        *val = kSnFlushedInit;
            break;
    }
    return Status::OK();
  }
}

Status MasterClient::SetWatermark(Watermark w, int64_t new_val) {
  PutRequest req;
  PutResponse res;
  req.set_key(WatermarkKey(chain_id_, w));
  req.set_value(std::to_string(new_val));
  auto status = etcd_->Put(req, &res);
  return Status::OK();
}

/**
 * Start a heartbeat with etcd. This is NOT THE SAME as registering as a chain member - it just tells etcd we're alive.
 * This heartbeat should continue as long as the process is running.
 * It can be used with other operations to time out if we lose contact with etcd, like with the lock in AcquireID.
 * @param channel
 */
void MasterClient::StartHeartbeat() {
  // Establish heartbeat.
  {
    LeaseGrantRequest req;
    req.set_ttl(kHeartbeatTimeoutSec);
    LeaseGrantResponse res;
    auto status = etcd_->LeaseGrant(req, &res);
    if (status.ok()) {
      heartbeat_lease_id_ = res.id();
    }
  }
  heartbeat_thread_ = std::unique_ptr<std::thread>(new std::thread(
      // No need to join - this should only die if the process dies.
      [this](int64_t lease_id, int hb_interval) {
        etcd::Client etcd(channel_);
        auto my_hb_key = HeartbeatKey(chain_id_, member_id_);
        while (true) {
          LeaseKeepAliveRequest req;
          req.set_id(heartbeat_lease_id_);
          LeaseKeepAliveResponse res;
          auto status = etcd.LeaseKeepAlive(req, &res);
          CHECK(status.ok())
          << "Failed to KeepAlive lease " << heartbeat_lease_id_ << ". "
          << "GRPC error " << status.error_code() << ": "
          << status.error_message();
          std::this_thread::sleep_for(std::chrono::seconds(hb_interval));
        }
      },
      heartbeat_lease_id_,
      kHeartbeatIntervalSec
  ));
  LOG(INFO) << "Started heartbeat."
            << "Interval: " << kHeartbeatIntervalSec << " seconds, "
            << "Timeout: " << kHeartbeatTimeoutSec << " seconds.";

}

/**
 * Pick a chain-unique ID for this member in order to join the given chain.
 * This unique member ID identifies this member within the chain, but IS NOT unique between multiple chains.
 * Must already be connected to etcd.
 * Steps:
 *   1. Lock credis/chain_id/last_id
 *   2. Increment the int64_t stored there - this is my new member ID.
 * @param chain_id
 * @param lock_id
 * @param lease_id
 * @return
 */
void MasterClient::AcquireID() {
  if (member_id_ != kUnsetMemberID) {
    LOG(INFO) << "Tried to join a chain but was already in one.";
    return;
  }
  // Any time we take a shared lock, we must include the heartbeat lease id.
  // If the heartbeat lease is not included, and we crash while holding the lock, THE LOCK IS LEAKED.
  LockRequest lock_req;
  lock_req.set_name(JoinLock(chain_id_));
  lock_req.set_lease(heartbeat_lease_id_);
  LockResponse lock_res;
  auto status = etcd_->Lock(lock_req, &lock_res);
  std::string lock_id = lock_res.key();

  // Get the last ID that was assigned.
  // The existence of a key named after the lock_id means we hold the join lock.
  TxnRequest get_id_req;
  etcd::util::MakeKeyExistsCompare(lock_id, get_id_req.add_compare());
  auto rr = new RangeRequest();
  rr->set_key(LastIDKey(chain_id_));
  get_id_req.add_success()->set_allocated_request_range(rr);
  TxnResponse get_id_res;
  status = etcd_->Transaction(get_id_req, &get_id_res);
  CHECK(get_id_res.succeeded())
  << "Tried to get last ID without holding the join lock. Is etcd running?";

  RangeResponse response = get_id_res.responses(0).response_range();
  if (response.count() > 0) {
    int64_t current_id = std::stol(response.kvs().Get(0).value());
    member_id_ = current_id + 1;
  } else {
    // No ID has ever been assigned for this chain, so the GET returned nothing.
    member_id_ = 0;
  }

  // This says, "If I am holding the lock, then write down in etcd that my ID was the last ID assigned."
  TxnRequest put_id_req;
  etcd::util::MakeKeyExistsCompare(lock_id, put_id_req.add_compare());
  auto pr = new PutRequest();
  pr->set_key(LastIDKey(chain_id_));
  pr->set_value(std::to_string(member_id_));
  put_id_req.add_success()->set_allocated_request_put(pr);
  TxnResponse put_id_res;
  status = etcd_->Transaction(put_id_req, &put_id_res);
  CHECK(put_id_res.succeeded())
  << "Tried to claim a new ID (" << member_id_
  << ") without holding the join lock. Is etcd running?";

  UnlockRequest unlock_req;
  unlock_req.set_key(lock_id);
  UnlockResponse unlock_res;
  status = etcd_->Unlock(unlock_req, &unlock_res);
  LOG(INFO) << "Acquired ID " << member_id_ << " in chain " << chain_id_;
}

/**
 * Register my member info by updating the value of my heartbeat key.
 * The key disappears if the member times out from etcd.
 */
void MasterClient::RegisterMemberInfo() {
  json my_conn_info = {
      {"address", redis_addr_},
      {"port", redis_port_},
      {"prev", prev_id_},
      {"next", next_id_},
      {"role", role_}
  };
  PutRequest req;
  PutResponse res;
  req.set_key(HeartbeatKey(chain_id_, member_id_));
  req.set_value(my_conn_info.dump());
  // include lease so that the key disappears if we time out from etcd.
  req.set_lease(heartbeat_lease_id_);
  auto status = etcd_->Put(req, &res);
  LOG(INFO) << "Registered new connection info: " << my_conn_info.dump()
            << " (rev " << res.header().revision() << ")";
}


// Start a thread to watch for changes to this member's configuration. If some
// are found, the thread updates the member's configuration by sending Redis
// commands on the loopback interface to the port given by OWN_PORT.
void MasterClient::StartWatchingConfig() {
  // Starts a thread to watch for updated config and apply it to the member.
  config_thread_ = std::unique_ptr<std::thread>(new std::thread(
      // No need to join - this should only die if the process dies.
      [this]() {
        auto etcd = std::shared_ptr<etcd::Client>(new etcd::Client(channel_));
        auto my_config_key = ConfigKey(chain_id_, member_id_);
        auto wcr = new WatchCreateRequest();
        wcr->set_key(my_config_key);
        wcr->set_progress_notify(true);
        wcr->set_range_end("");
        WatchRequest watch_req;
        watch_req.set_allocated_create_request(wcr);
        WatchResponse watch_res;
        etcd::WatchStreamPtr changes = etcd->MakeWatchStream(watch_req,
                                                          &watch_res);
        LOG(INFO) << "Started watching configs at " << my_config_key;
        WatchResponse wr;
        int64_t last_rev_seen = wr.header().revision();
        while (true) {
          CHECK(changes->Read(&wr))
          << "Failed read from watch stream for" << my_config_key;
          // There would be 0 events if it was just a progress_notify.
          if (wr.events_size() > 0) {
            Event latest_event = wr.events().Get(wr.events_size() - 1);
            last_rev_seen = latest_event.kv().mod_revision();
            if (latest_event.type() == Event_EventType_PUT) {
                HandleConfigPut(latest_event);
            }
          }
        }
      }
  ));
}

void MasterClient::HandleConfigPut(Event put_event) {
  CHECK(put_event.type() == Event_EventType_PUT)
  << "HandleConfigPut must be called on a PUT event";
  auto my_config = json::parse(put_event.kv().value());
  std::string role = my_config["role"];
  int64_t prev_id = my_config["prev"];
  int64_t next_id = my_config["next"];

  if (role == chain::kRoleSingleton) {
    RoleOptions options;
    options.role = role;
    options.prev_address = std::to_string(chain::kNoMember);
    options.prev_port = -1;
    options.next_address = std::to_string(chain::kNoMember);
    options.next_port = -1;
    options.sn = -1;
    options.drop_writes = false;
    SetOwnRole(options);
    role_ = role;
    prev_id_ = prev_id;
    next_id_ = next_id;
    RegisterMemberInfo();
    return;
  }

  // Build and run an etcd transaction to get reported config of the new head
  // and new tail. If a heartbeat key we expected to be there doesn't exist, the
  // transaction will not succeed.
  TxnRequest tx_req;
  std::string prev_hb_key, next_hb_key;
  bool has_next = (role != chain::kRoleTail && role != chain::kRoleSingleton);
  if (has_next) {
    next_hb_key = HeartbeatKey(chain_id_, next_id);
    etcd::util::MakeKeyExistsCompare(next_hb_key, tx_req.add_compare());
    auto range_req = new RangeRequest();
    range_req->set_key(next_hb_key);
    tx_req.add_success()->set_allocated_request_range(range_req);
  }

  bool has_prev = (role != chain::kRoleHead && role != chain::kRoleSingleton);
  if (has_prev) {
    prev_hb_key = HeartbeatKey(chain_id_, prev_id);
    etcd::util::MakeKeyExistsCompare(prev_hb_key, tx_req.add_compare());
    auto range_req = new RangeRequest();
    range_req->set_key(prev_hb_key);
    tx_req.add_success()->set_allocated_request_range(range_req);
  }

  TxnResponse tx_res;
  auto status = etcd_->Transaction(tx_req, &tx_res);
  if (tx_res.succeeded()) {
    // Block until the new successor updates its reported state.
    if (has_next) {
      WaitForNewChild(next_id);
    }

    // Parse config info out of the responses and figure out where to connect
    // to the new parent and child.
    RoleOptions options;
    for (const auto& res : tx_res.responses()) {
      auto res_kv = res.response_range().kvs().Get(0);
      auto their_config = json::parse(res_kv.value());
      if (res_kv.key() == prev_hb_key) {
        options.prev_address = their_config["address"];
        options.prev_port = their_config["port"];
      } else if (res_kv.key() == next_hb_key) {
        options.next_address = their_config["address"];
        options.next_port = their_config["port"];
      }
    }
    options.role = role;
    options.sn = -1;
    options.drop_writes = false;
    SetOwnRole(options);

    if (has_next) {
      EnableReplication();
      UnblockWrites();
    }

    role_ = role;
    prev_id_ = prev_id;
    next_id_ = next_id;
    RegisterMemberInfo();
  } else {
    // TODO: retry with backoff.
    LOG(INFO) << "Attempt to read new config failed. Is the new parent or new child down?";
  }
}

void MasterClient::SetOwnRole(const RoleOptions& options) {
  // Set my own role by sending myself a Redis command.
  redisContext* redis_ctx = redisConnect(redis_addr_.c_str(), redis_port_);
  auto set_role = reinterpret_cast<redisReply*>(redisCommand(
      redis_ctx,
      "MEMBER.SET_ROLE %s %s %s %s %s %s %s",
      options.role.c_str(),
      options.prev_address.c_str(),
      std::to_string(options.prev_port).c_str(),
      options.next_address.c_str(),
      std::to_string(options.next_port).c_str(),
      std::to_string(options.sn).c_str(),
      std::to_string(options.drop_writes).c_str()
  ));
  freeReplyObject(set_role);
  CHECK(redis_ctx->err == REDIS_OK)
  << "MEMBER.SET_ROLE failed with error: " << redis_ctx->errstr;
}

void MasterClient::EnableReplication() {
  redisContext* redis_ctx = redisConnect(redis_addr_.c_str(), redis_port_);
  auto replicate = reinterpret_cast<redisReply*>(
      redisCommand(redis_ctx, "MEMBER.REPLICATE"));
  freeReplyObject(replicate);
  CHECK(redis_ctx->err == REDIS_OK)
  << "MEMBER.REPLICATE failed with error: " << redis_ctx->errstr;

}
void MasterClient::UnblockWrites() {
  redisContext* redis_ctx = redisConnect(redis_addr_.c_str(), redis_port_);
  auto unblock_writes = reinterpret_cast<redisReply*>(
      redisCommand(redis_ctx, "MEMBER.UNBLOCK_WRITES"));
  freeReplyObject(unblock_writes);
  CHECK(redis_ctx->err == REDIS_OK)
  << "MEMBER.UNBLOCK_WRITES failed with error: " << redis_ctx->errstr;
}

void MasterClient::WaitForNewChild(int64_t child_id) {
  auto child_hb_key = HeartbeatKey(chain_id_, child_id);
  backoff::ExponentialBackoff(
      [this, child_hb_key] ()->bool {
        RangeRequest req;
        req.set_key(child_hb_key);
        req.set_range_end("");
        req.set_revision(-1);
        RangeResponse res;
        auto status = etcd_->Range(req, &res);
        if (status.ok() && res.kvs_size() > 0) {
          auto next_config = json::parse(res.kvs().Get(0).value());
          if (next_config["prev"] == member_id_) {
            LOG(INFO)
                << "New child reports us as its parent - forwarding writes...";
            return true;
          }
        }
        LOG(INFO) << "New child @" << child_hb_key
                  << " has not reported us as its parent yet. Waiting...";
        return false;
      },
      1000 * kHeartbeatIntervalSec,
      1000 * kHeartbeatTimeoutSec,
      2
  );
}
