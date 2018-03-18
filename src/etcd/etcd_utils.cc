
#include <string>
#include "etcd3/include/etcd3.h"
#include "etcd_utils.h"

namespace utils {
EtcdURL split_etcd_url(std::string url) {
  EtcdURL result;
  result.url = url;

  auto start_of_chain_prefix = url.find_first_of('/');
  if (start_of_chain_prefix == std::string::npos) {
    result.chain_prefix = "/";
  } else {
    result.chain_prefix =
        url.substr(start_of_chain_prefix, std::string::npos);
  }
  auto address = url.substr(0, start_of_chain_prefix);
  result.address = address;

  auto start_of_port = address.find_first_of(':') + 1;
  auto host = address.substr(0, start_of_port - 1);
  auto port_str = address.substr(start_of_port);
  result.host = host;
  result.port = std::stoi(port_str);
  return result;
}

//ObservableRange::ObservableRange(std::shared_ptr<grpc::Channel> channel,
//                                 etcd3::pb::RangeRequest initial_request,
//                                 ObserverFunc handler)
//    : channel_(channel),
//      initial_request_(std::move(initial_request)),
//      handler_(handler) {}
//
//grpc::Status ObservableRange::GetInitialState(etcd3::pb::RangeResponse* res) {
//  etcd3::Client etcd(channel_);
//
//  start_revision_ = -1;
//  auto status = etcd.Range(initial_request_, res);
//  if (!status.ok()) {
//    return status;
//  }
//  for (auto kv : res->kvs()) {
//    if (start_revision_ == -1 || kv.mod_revision() < start_revision_) {
//      start_revision_ = kv.mod_revision();
//    }
//  }
//  return grpc::Status();
//}
//
//grpc::Status ObservableRange::Observe(bool reset_start_revision) {
//  etcd3::Client etcd(channel_);
//
//  if (reset_start_revision) {
//    etcd3::pb::RangeResponse initial_response;
//    auto status = GetInitialState(&initial_response);
//    if (!status.ok()) {
//      return status;
//    }
//  }
//
//  auto wcr = new etcd3::pb::WatchCreateRequest();
//  wcr->set_key(initial_request_.key());
//  wcr->set_range_end(initial_request_.range_end());
//  wcr->set_start_revision(start_revision_);
//  auto changes = etcd.MakeWatchStream(wcr);
//
//  etcd3::pb::WatchResponse response;
//  while (changes->Read(&response)) {
//    handler_(response.events());
//  }
//  return grpc::Status::CANCELLED;
//}
}
