#ifndef CREDIS_COORDINATOR_H_
#define CREDIS_COORDINATOR_H_

#include <iostream>

#include "nlohmann/json.hpp"
#include "glog/logging.h"
#include "hiredis/hiredis.h"
#include "etcd/etcd.h"
#include "chain.h"

using namespace chain;

class Coordinator {
public:
    struct Options {
        bool auto_add_new_members;
    };
    explicit Coordinator(std::unique_ptr<etcd::ClientInterface> etcd);
    Coordinator(std::unique_ptr<etcd::ClientInterface> etcd, Options options);
    grpc::Status ManageChain(std::string chain_id);
private:
  int64_t WriteChain(Chain* chain);
  grpc::Status ListenForChanges(Chain* chain);

  bool IsSystemKey(const std::string& key);
  bool IsHeartbeatKey(const std::string& key);
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

#endif  // CREDIS_COORDINATOR_H_
