#ifndef CREDIS_ETCD_MASTER_H_
#define CREDIS_ETCD_MASTER_H_

#include <iostream>

#include "nlohmann/json.hpp"
#include "glog/logging.h"
#include "hiredis/hiredis.h"
#include "etcd/etcd.h"
#include "chain.h"

using namespace chain;

class EtcdMaster {
 public:
  struct Options {
    bool auto_add_new_members;
  };
  explicit EtcdMaster(std::unique_ptr<etcd::ClientInterface> etcd);
  EtcdMaster(std::unique_ptr<etcd::ClientInterface> etcd, Options options);
  grpc::Status ManageChain(std::string chain_id);
  static Chain ReadChain(const etcd::ClientInterface& etcd,
                         std::string chain_id);
  static bool IsSystemKey(const std::string& key);
  static bool IsHeartbeatKey(const std::string& key);
 private:
  int64_t WriteChain(Chain* chain);
  grpc::Status ListenForChanges(Chain* chain);

  static const std::string kKeyPrefix;
  static const std::string kKeyTypeHeartbeat;

  Options options_;
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<etcd::ClientInterface> etcd_;
  std::unique_ptr<
      grpc::ClientReaderWriterInterface<
          WatchRequest, WatchResponse>> changes_;
  // TODO: multiple chain support
  // std::string chain_ids;
};

#endif  // CREDIS_ETCD_MASTER_H_
