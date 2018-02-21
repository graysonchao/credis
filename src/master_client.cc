#include <chrono>
#include <string>
#include <thread>

#include "master_client.h"
#include "glog/logging.h"

using namespace etcd;

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
    const std::string& chain_id
) {
  auto channel = grpc::CreateChannel(address + ":" + std::to_string(port), grpc::InsecureChannelCredentials());
  etcd_client_ = std::unique_ptr<EtcdClient>(new EtcdClient(channel));
  chain_id_ = chain_id;
  // Start a lease with etcd. Sending a KeepAlive for this lease implements a heartbeat.
  StartHeartbeat(channel);
  // While holding a chain-wide lock, acquire the next available chain member ID and claim it for myself.
  AcquireID(chain_id_);
  // Register myself as a member of the chain by putting my member ID as a key. The key disappears if heartbeat expires.
  RegisterAsMember(chain_id_);
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
 * Return the name of the key containing the last assigned ID for the given chain.
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

const std::string MasterClient::ConnInfoKey(std::string chain_id) const {
  CHECK(member_id_ != kUnsetMemberID) << "Member can't get its connection info key if it doesn't know its member ID.";
  return kKeyPrefix + chain_id + "/" + std::to_string(member_id_);
}

Status MasterClient::GetWatermark(Watermark w, int64_t* val) const {
  std::string key = WatermarkKey(chain_id_, w);
  std::unique_ptr<RangeResponse> response = etcd_client_->Range(key, "");

  if (response->kvs_size() > 0) {
    auto kvs = response->kvs();
    const KeyValue &result = kvs.Get(0);
    *val = std::stol(result.value());
    LOG(INFO) << "(etcd) GET " << WatermarkKey(chain_id_, w) << ": " << result.value();
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

  LOG(INFO) << "(etcd) PUT " << WatermarkKey(chain_id_, w) << ": " << new_val
            << " (was " << prev_value << ")";

  return Status::OK();
}

/**
 * Start a heartbeat with etcd. This is NOT THE SAME as registering as a chain member - it just tells etcd we're alive.
 * This heartbeat should continue as long as the process is running.
 * It can be used with other operations to time out if we lose contact with etcd, like with the lock in AcquireID.
 * @param channel
 */
void MasterClient::StartHeartbeat(std::shared_ptr<grpc::Channel> channel) {
  // Establish heartbeat.
  auto lease_grant = etcd_client_->LeaseGrant(kHeartbeatTimeoutSec);
  heartbeat_lease_id_ = lease_grant->id();
  heartbeat_thread_ = std::thread(
      // No need to join - this should only die if the process dies.
      [channel](int64_t lease_id, int hb_interval) {
          auto etcd = std::unique_ptr<EtcdClient>(new EtcdClient(channel));
          while (true) {
            etcd->LeaseKeepAlive(lease_id);
            std::this_thread::sleep_for(std::chrono::seconds(hb_interval));
          }
      },
      heartbeat_lease_id_,
      kHeartbeatIntervalSec
  );
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
void MasterClient::AcquireID(std::string chain_id) {
  if (member_id_ != kUnsetMemberID) {
    LOG(INFO) << "Tried to join a chain but was already in one.";
    return;
  }
  // PLEASE ensure that any time you take a lock, you include the heartbeat lease id.
  // If the heartbeat lease is not included, and you crash while holding the lock, THE LOCK IS PERMANENTLY LOCKED.
  auto lock_response = etcd_client_->Lock(JoinLock(chain_id), heartbeat_lease_id_);
  std::string lock_id = lock_response->key();

  // RequestOp takes ownership and frees this RangeRequest.
  auto rr = new RangeRequest();
  rr->set_key(LastIDKey(chain_id));
  rr->set_range_end("");
  RequestOp get_last_id;
  get_last_id.set_allocated_request_range(rr);
  // Transactions are used by etcd to guard operations based on a condition.
  // This says, "If I am holding the lock $lock_id, then find out the last ID that was assigned."
  std::vector<Compare> compare_lock_exists = {*etcd::txn::BuildKeyExistsComparison(lock_id)};
  std::vector<RequestOp> success_ops = {get_last_id};
  std::vector<RequestOp> failure_ops = {};
  auto get_tx = etcd_client_->Transaction(
      compare_lock_exists,
      success_ops,
      failure_ops
  );
  CHECK(get_tx->succeeded()) << "Tried to get the last assigned ID without holding the join lock. Is etcd running?";

  RangeResponse response = get_tx->responses(0).response_range();
  if (response.count() > 0) {
    int64_t current_id = std::stol(response.kvs().Get(0).value());
    member_id_ = current_id + 1;
  } else {
    // No ID has ever been assigned for this chain, so the GET returned nothing.
    member_id_ = 0;
  }

  // RequestOp takes ownership and frees this PutRequest.
  auto pr = new PutRequest();
  pr->set_key(LastIDKey(chain_id));
  pr->set_value(std::to_string(member_id_));
  RequestOp put_new_id;
  put_new_id.set_allocated_request_put(pr);
  success_ops = {put_new_id};
  failure_ops = {};
  // This says, "If I am holding the lock, then write down in etcd that my ID was the last ID assigned."
  auto put_tx = etcd_client_->Transaction(
    compare_lock_exists,
    success_ops,
    failure_ops
  );
  CHECK(put_tx->succeeded())
    << "Tried to claim a new ID (" << member_id_ << ") without holding the join lock. Is etcd running?";

  auto unlock_response = etcd_client_->Unlock(lock_id);
  LOG(INFO) << "Acquired ID " << member_id_ << " in chain " << chain_id;
}

/**
 * Register as a chain member by putting my unique member ID as a key under the chain's config.
 * For example, if I am joining chain "foobar", and this.member_id_ = 1, this puts the key "credis/foobar/1".
 * The key contents are currently unimportant, but could be used to store (for example) connection info.
 * The PRESENCE of the specific member key tells the master if this client is alive.
 * We include heartbeat_lease_id_ in the etcd call so it will disappear if we time out from etcd.
 * @param chain_id
 */
void MasterClient::RegisterAsMember(std::string chain_id) {
  auto put_key = etcd_client_->Put(
      ConnInfoKey(chain_id),
      "Yet for all that, flowers fall amid our regret and yearning, and hated weeds grow apace.",
      // - Dogen, founder of the Soto Zen School, in the Genjokoan
      heartbeat_lease_id_
  );
}

