#include <chrono>
#include <string>
#include <thread>

#include "nlohmann/json.hpp"
#include "glog/logging.h"
#include "hiredis/hiredis.h"

#include "master_client.h"

using namespace etcd;
using json = nlohmann::json;

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
  etcd_client_ = std::unique_ptr<EtcdClient>(new EtcdClient(channel_));
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
  CHECK(member_id_ != kUnsetMemberID) << "Member can't get its heartbeat key if it doesn't know its member ID.";
  return kKeyPrefix + chain_id + "/" + std::to_string(member_id) + "/hb";
}

/**
 * Return the config key for this member
 * @param chain_id
 * @return
 */
const std::string MasterClient::ConfigKey(std::string chain_id, int64_t member_id) const {
  CHECK(member_id_ != kUnsetMemberID) << "Member can't get its config key if it doesn't know its member ID.";
  return kKeyPrefix + chain_id + "/" + std::to_string(member_id) + "/config";
}

Status MasterClient::GetWatermark(Watermark w, int64_t* val) const {
  std::string key = WatermarkKey(chain_id_, w);
  std::unique_ptr<RangeResponse> response = etcd_client_->Range(key, "");

  if (response->kvs_size() > 0) {
    auto kvs = response->kvs();
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
  std::unique_ptr<PutResponse> response = etcd_client_->Put(
      WatermarkKey(chain_id_, w),
      std::to_string(new_val),
      0,
      true
  );
  std::string prev_value = response->prev_kv().value();
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
  auto lease_grant = etcd_client_->LeaseGrant(kHeartbeatTimeoutSec);
  heartbeat_lease_id_ = lease_grant->id();
  heartbeat_thread_ = std::unique_ptr<std::thread>(new std::thread(
      // No need to join - this should only die if the process dies.
      [this](int64_t lease_id, int hb_interval) {
          auto etcd = std::unique_ptr<EtcdClient>(new EtcdClient(channel_));
          while (true) {
            etcd->LeaseKeepAlive(lease_id);
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
  auto lock_response = etcd_client_->Lock(JoinLock(chain_id_), heartbeat_lease_id_);
  std::string lock_id = lock_response->key();

  // Transactions are used by etcd to guard operations based on a condition.
  // This says, "If I am holding the lock $lock_id, then find out the last ID that was assigned."
  std::vector<Compare> compare_lock_exists = {*etcd::txn::BuildKeyExistsComparison(lock_id)};
  std::vector<RequestOp> success_ops = {*etcd::txn::BuildGetRequest(LastIDKey(chain_id_))};
  std::vector<RequestOp> failure_ops = {};
  auto get_tx = etcd_client_->Transaction(compare_lock_exists, success_ops, failure_ops);
  CHECK(get_tx->succeeded()) << "Tried to get the last assigned ID without holding the join lock. Is etcd running?";

  RangeResponse response = get_tx->responses(0).response_range();
  if (response.count() > 0) {
    int64_t current_id = std::stol(response.kvs().Get(0).value());
    member_id_ = current_id + 1;
  } else {
    // No ID has ever been assigned for this chain, so the GET returned nothing.
    member_id_ = 0;
  }

  // This says, "If I am holding the lock, then write down in etcd that my ID was the last ID assigned."
  success_ops = {*etcd::txn::BuildPutRequest(LastIDKey(chain_id_), std::to_string(member_id_))};
  failure_ops = {};
  auto put_tx = etcd_client_->Transaction(compare_lock_exists, success_ops, failure_ops);
  CHECK(put_tx->succeeded())
    << "Tried to claim a new ID (" << member_id_ << ") without holding the join lock. Is etcd running?";

  auto unlock_response = etcd_client_->Unlock(lock_id);
  LOG(INFO) << "Acquired ID " << member_id_ << " in chain " << chain_id_;
}

/**
 * Register my member info by updating the value of my heartbeat key.
 * The key disappears if the member times out from etcd.
 */
void MasterClient::RegisterMemberInfo() {
  json my_conn_info = {
      {"address", redis_addr_},
      {"port", std::to_string(redis_port_)},
      {"prev", prev_id_},
      {"next", next_id_},
      {"role", role_}
  };
  auto put_key = etcd_client_->Put(
      HeartbeatKey(chain_id_, member_id_),
      my_conn_info.dump(),
      // include lease in the etcd call so that the key disappears if we time out from etcd.
      heartbeat_lease_id_
  );
  LOG(INFO) << "Registered new connection info: " << my_conn_info.dump();
}

/**
 * Start a thread to watch for changes to this member's configuration. If some are found, the thread updates the
 * member's configuration by sending RESP commands on the loopback interface to the port given by OWN_PORT.
 * @param chain_id
 * @param own_port
 */
void MasterClient::StartWatchingConfig() {
  // Starts a thread to watch for updated configuration and apply it to the member.
  config_thread_ = std::unique_ptr<std::thread>(new std::thread(
      // No need to join - this should only die if the process dies.
      [this]() {
          auto etcd = std::shared_ptr<EtcdClient>(new EtcdClient(channel_));
          auto changes = etcd->WatchCreate(
              ConfigKey(chain_id_, member_id_),
              /*range_end=*/"",
              /*start_revision=*/-1,
              /*progress_notify=*/true,
              /*filters=*/{},
              /*prev_kv=*/false
          );
          LOG(INFO) << "Started watching configs at " << ConfigKey(chain_id_, member_id_);
          WatchResponse wr;
          changes->WaitForInitialMetadata();
          changes->Read(&wr);
          // The first response is to the CreateRequest.
          int64_t last_rev_seen = wr.header().revision();
          while (true) {
            if (!changes->Read(&wr)) {
              // changes = etcd->WatchCreate(ConfigKey(chain_id_, member_id_), "", -1, true, {}, false);
              break;
            }
            // There would be 0 events if it was just a progress_notify
            // (see https://coreos.com/etcd/docs/latest/learning/api.html#watch-streams)
            if (wr.events_size() > 0) {
              Event latest_event = wr.events().Get(wr.events_size() - 1);
              last_rev_seen = latest_event.kv().mod_revision();
              if (latest_event.type() == Event_EventType_PUT) {
                HandleConfigPut(latest_event, etcd);
              } else {
                // TODO: leave the cluster if config file is deleted.
              }
            }
          }
      }
    ));
}

void MasterClient::HandleConfigPut(Event put_event, std::shared_ptr<etcd::EtcdClient> etcd) {
  CHECK(put_event.type() == Event_EventType_PUT) << "HandleConfigPut must be called on a PUT event";
  auto my_config = json::parse(put_event.kv().value());
  std::string my_role = my_config["role"];
  std::string prev_id = my_config["prev"];
  std::string next_id = my_config["next"];

  if (my_role == "singleton") {
    auto redisCtx = std::unique_ptr<redisContext>(redisConnect(redis_addr_.c_str(), redis_port_));
    redisReply* reply = reinterpret_cast<redisReply*>(redisCommand(
        redisCtx.get(),
        "MEMBER.SET_ROLE %s %s %s %s %s %s %s",
        my_role.c_str(), "nil", "nil", "nil", "nil", "-1", "0"
    ));
    freeReplyObject(reply);
    return;
  }

  // Arguments to MEMBER.SET_ROLE.
  std::string prev_address = "nil";
  std::string next_address = "nil";
  std::string prev_port = "nil";
  std::string next_port = "nil";
  std::string sn_string = "-1";
  std::string drop_writes_string = "0";

  // Build and run an etcd transaction to get reported config of the new head and new tail.
  // If a heartbeat key we expected to be there doesn't exist, the transaction will not succeed.
  std::vector<Compare> comparisons;
  std::vector<RequestOp> success_ops;
  std::vector<RequestOp> failure_ops = {};
  bool has_next = (my_role != "tail" && my_role != "singleton");
  bool has_prev = (my_role != "head" && my_role != "singleton");
  std::string prev_hb_key, next_hb_key;
  if (has_next) {
    next_hb_key = HeartbeatKey(chain_id_, std::stol(next_id));
    comparisons.push_back(*etcd::txn::BuildKeyExistsComparison(next_hb_key));
    success_ops.push_back(*etcd::txn::BuildGetRequest(next_hb_key));
  }
  if (has_prev) {
    prev_hb_key = HeartbeatKey(chain_id_, std::stol(prev_id));
    comparisons.push_back(*etcd::txn::BuildKeyExistsComparison(prev_hb_key));
    success_ops.push_back(*etcd::txn::BuildGetRequest(prev_hb_key));
  }
  auto tx = etcd->Transaction(comparisons, success_ops, failure_ops);
  if (tx->succeeded()) {
    // Parse config info out of the responses.
    for (const auto &res : tx->responses()) {
      auto res_kv = res.response_range().kvs().Get(0);
      auto their_config = json::parse(res_kv.value());
      if (res_kv.key() == prev_hb_key) {
        prev_address = their_config["address"];
        prev_port = their_config["port"];
      } else if (res_kv.key() == next_hb_key) {
        next_address = their_config["address"];
        next_port = their_config["port"];
      }
    }
  } else {
    // TODO: retry with backoff.
    LOG(INFO) << "Attempt to read new config failed. Is the new parent or new child down?";
  }

  // Block until the new successor updates its reported state.
  if (has_next) {
    int64_t backoff_sec = 1;
    while (true) {
      auto next_config_req = etcd->Range(next_hb_key, "", -1);
      if (next_config_req->kvs_size() > 0) {
        auto next_config = json::parse(next_config_req->kvs().Get(0).value());
        if (next_config["prev"] == std::to_string(member_id_)) {
          LOG(INFO) << "New child reports us as its parent - forwarding writes...";
          break;
        }
      }
      LOG(INFO) << "New child " << next_id << " has not reported us as its parent yet. "
                << "Waiting " << backoff_sec << "sec";
      std::this_thread::sleep_for(std::chrono::seconds(backoff_sec));
      backoff_sec = backoff_sec * 2;
    }
  }

  // Set my own role by sending myself a Redis command.
  redisContext* redis_ctx = redisConnect(redis_addr_.c_str(), redis_port_);
  auto reply = reinterpret_cast<redisReply*>(redisCommand(
      redis_ctx,
      "MEMBER.SET_ROLE %s %s %s %s %s %s %s",
      my_role.c_str(),
      prev_address.c_str(),
      prev_port.c_str(),
      next_address.c_str(),
      next_port.c_str(),
      sn_string.c_str(),
      drop_writes_string.c_str()
  ));

  // If we were able to successfully set our own role, then update our config in etcd.
  if (redis_ctx->err == 0) {
    role_ = my_role;
    prev_id_ = prev_id;
    next_id_ = next_id;
    RegisterMemberInfo();
  } else {
    LOG(INFO) << "MEMBER.SET_ROLE failed with error: " << redis_ctx->errstr;
  }
  freeReplyObject(reply);
}
