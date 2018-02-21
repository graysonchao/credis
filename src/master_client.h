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
      const int port,
      const std::string& chain_id = "foobar"
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

 private:
  const std::string WatermarkKey(std::string chain_id, Watermark w) const;
  const std::string LastIDKey(std::string chain_id) const;
  const std::string ConnInfoKey(std::string chain_id) const;
  const std::string JoinLock(std::string chain_id) const;
  void StartHeartbeat(std::shared_ptr<grpc::Channel> channel);
  void AcquireID(std::string chain_id);
  void RegisterAsMember(std::string chain_id);

  static constexpr int64_t kSnCkptInit = 0;
  static constexpr int64_t kSnFlushedInit = 0;
  static constexpr int64_t kUnsetMemberID = -1;
  const std::string kKeyPrefix = "credis/";
  const int kHeartbeatIntervalSec = 5;
  const int kHeartbeatTimeoutSec = 20;

  std::unique_ptr<etcd::EtcdClient> etcd_client_;
  std::thread heartbeat_thread_;
  std::string chain_id_;
  int64_t heartbeat_lease_id_;
  int64_t member_id_ = kUnsetMemberID;

};

#endif  // CREDIS_MASTER_CLIENT_H_
