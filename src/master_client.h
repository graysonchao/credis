#ifndef CREDIS_MASTER_CLIENT_H_
#define CREDIS_MASTER_CLIENT_H_

// A client for all chain nodes to talk to the master.
//
// The default implementation assumes a redis-based master.  It is possible that
// in the future, this interface can be backed by other implementation, such as
// etcd or consul.

#include <memory>
#include <string>
#include <thread>

extern "C" {
#include "hiredis/hiredis.h"
}

#include "leveldb/db.h"
#include "etcd/etcd.h"

using Status = leveldb::Status;

class MasterClient {
 public:
  enum class Watermark : int {
    kSnCkpt = 0,
    kSnFlushed = 1,
  };


  /**
   * Connect to the master, and join the specified chain.
   * @param address
   * @param port
   * @param chain_id
   * @return
   */
  Status Connect(
      const std::string& address,
      int port,
      const std::string& chain_id,
      const std::string& redis_addr,
      int own_port
  );

  // TODO(zongheng): impl.
  // Retries the current head and tail nodes (for writes and reads,
  // respectively).
  Status Head(std::string* address, int* port);
  Status Tail(std::string* address, int* port);

  // Watermark sequence numbers
  //
  // The master manages and acts as the source-of-truth for watermarks.
  //
  // Definitions:
  //   sn_ckpt: next/smallest sn yet to be checkpointed;
  //            thus, [0, sn_ckpt) is the currently checkpointed range.
  //   sn_flushed: next/smallest sn yet to be flushed;
  //            thus, [0, sn_flushed) is the currently flushed range.
  //
  // Properties of various watermarks (and their extreme cases):
  //   sn_ckpt <= sn_latest_tail + 1 (i.e., everything has been checkpointed)
  //   sn_flushed <= sn_ckpt (i.e., all checkpointed data has been flushed)
  Status GetWatermark(Watermark w, int64_t* val) const;
  Status SetWatermark(Watermark w, int64_t new_val);

  static const std::string kKeyPrefix;// = "credis/";
  static const int kHeartbeatIntervalSec;// = 5;
  static const int kHeartbeatTimeoutSec;// = 20;
  static const int kHeartbeatBackoffMultiplier;// = 2;

 private:
  const std::string WatermarkKey(std::string chain_id, Watermark w) const;
  const std::string LastIDKey(std::string chain_id) const;
  const std::string JoinLock(std::string chain_id) const;
  const std::string HeartbeatKey(std::string chain_id, int64_t member_id) const;
  const std::string ConfigKey(std::string chain_id, int64_t member_id) const;
  void StartHeartbeat();
  void AcquireID();
  void RegisterMemberInfo();
  void StartWatchingConfig();

  void HandleConfigPut(Event e, std::shared_ptr<etcd::Client> etcd);

  static constexpr int64_t kSnCkptInit = 0;
  static constexpr int64_t kSnFlushedInit = 0;
  static constexpr int64_t kUnsetMemberID = -1;

  std::unique_ptr<etcd::Client> etcd_client_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<std::thread> heartbeat_thread_;
  std::unique_ptr<std::thread> config_thread_;
  std::string chain_id_;
  int64_t heartbeat_lease_id_;
  int64_t member_id_ = kUnsetMemberID;
  std::string role_ = "uninitialized";
  std::string prev_id_ = "-1";
  std::string next_id_ = "-1";
  std::string redis_addr_;
  int redis_port_;

};

enum class ChainRole : int {
    // 1-node chain: serves reads and writes.
    kSingleton = 0,
    // Values below imply # nodes in chain > 1.
    kHead = 1,
    kMiddle = 2,
    kTail = 3,
};

#endif  // CREDIS_MASTER_CLIENT_H_
