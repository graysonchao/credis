
#ifndef CREDIS_UTILS_H
#define CREDIS_UTILS_H

#include <vector>
#include "etcd3/include/etcd3.h"

namespace utils {

  class EtcdURL {
   public:
    std::string url;
    std::string address; // host:port
    std::string host;
    int port;
    std::string chain_prefix;
  };
  EtcdURL split_etcd_url(std::string url);

// A function that does something with the updated values of the observed range.
//typedef std::function<void(std::vector<etcd3::pb::Event>)> ObserverFunc;
//
//// An etcd Range that can be "observed": a handler can be registered that
//// is called on any updated values of keys in the Range.
//class ObservableRange {
// public:
//  // The initial request describes the range of keys to watch.
//  ObservableRange(std::shared_ptr<grpc::Channel> channel,
//                  etcd3::pb::RangeRequest initial_request,
//                  ObserverFunc handler);
//
//  // Get the initial state of the range, setting revision to watch from in the
//  // process.
//  grpc::Status GetInitialState(etcd3::pb::RangeResponse* res);
//  // Start watching the specified range, calling the handler on any updated
//  // values.
//  grpc::Status Observe(bool reset_start_revision);
//
// private:
//  std::shared_ptr<grpc::Channel> channel_;
//  etcd3::pb::RangeRequest initial_request_;
//  int64_t start_revision_;
//  ObserverFunc handler_;
//};
};

#endif //CREDIS_UTILS_H
