#ifndef CREDIS_ETCD_MASTER_H_
#define CREDIS_ETCD_MASTER_H_

#include <iostream>
#include <src/etcd/etcd_utils.h>

#include "glog/logging.h"
#include "hiredis/hiredis.h"
#include "nlohmann/json.hpp"
#include "etcd3/include/etcd3.h"
#include "src/chain.h"

using namespace chain;

using WatchRequest = etcd3::pb::WatchRequest;
using WatchResponse = etcd3::pb::WatchResponse;

class EtcdMaster {
 public:
  struct Options {
    bool auto_add_new_members;
  };
  EtcdMaster();
  explicit EtcdMaster(std::shared_ptr<grpc::Channel> channel);
 private:
  // Respond to updates in etcd state.
  void HandleStateChange(std::vector<etcd3::pb::Event> updates);
  int64_t WriteChain(Chain* chain);
  grpc::Status WatchChain(std::string chain_prefix);
  static const std::string kKeyTypeHeartbeat;

  Options options_;

  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<
      grpc::ClientReaderWriterInterface<
          WatchRequest, WatchResponse>> changes_;
  // TODO: multiple chain support
  // std::string chain_ids;
};

#endif  // CREDIS_ETCD_MASTER_H_
