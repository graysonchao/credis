//
// Created by Grayson Chao on 2/17/18.
//

#include <future>
#include <memory>
#include "etcd.h"

using namespace etcdserverpb;
using namespace mvccpb;

etcd::EtcdClient::EtcdClient(std::shared_ptr<grpc::ChannelInterface> channel)
        : kv_stub_(etcdserverpb::KV::NewStub(channel)),
          watch_stub_(etcdserverpb::Watch::NewStub(channel)),
          lease_stub_(etcdserverpb::Lease::NewStub(channel)) {}


std::unique_ptr<PutResponse> etcd::EtcdClient::Put(
    grpc::Status &status,
    const std::string key,
    const std::string value,
    const int64_t lease,
    const bool prev_key,
    const bool ignore_value,
    const bool ignore_lease
) {
    grpc::ClientContext context;
    PutRequest req;
    req.set_key(key);
    req.set_value(key);
    req.set_lease(lease);
    req.set_prev_kv(prev_key);
    req.set_ignore_value(ignore_value);

    auto res = std::unique_ptr<PutResponse>(new PutResponse());
    status = kv_stub_->Put(&context, req, res.get());
    return res;
}

std::unique_ptr<PutResponse> etcd::EtcdClient::Put(
    const std::string key,
    const std::string value,
    const int64_t lease,
    const bool prev_key,
    const bool ignore_value,
    const bool ignore_lease
) {
    grpc::Status status;
    return etcd::EtcdClient::Put(status, key, value, lease, prev_key, ignore_value, ignore_lease);
}

/**
 * Query etcd for a key or a range of keys.
 * Returns a unique_ptr to a RangeResponse.
 * @param key
 * @param range_end
 * @param revision
 * @param status a grpc::Status. Will store the return code - check it for errors
 * @return
 */
std::unique_ptr<RangeResponse> etcd::EtcdClient::Range(
    const std::string &key,
    const std::string &range_end,
    const int64_t revision
) {
    grpc::Status status;
    return etcd::EtcdClient::Range(status, key, key, revision);
}

/**
 * Query etcd for a key or a range of keys.
 * Returns a unique_ptr to a RangeResponse.
 * @param key
 * @param range_end
 * @param revision
 * @return
 */
std::unique_ptr<RangeResponse> etcd::EtcdClient::Range(
    grpc::Status &status,
    const std::string &key,
    const std::string &range_end,
    const int64_t revision
) {
    grpc::ClientContext context;
    RangeRequest req;
    req.set_key(key);
    req.set_range_end(range_end);
    req.set_revision(revision);

    auto res = std::unique_ptr<RangeResponse>(new RangeResponse());
    status = kv_stub_->Range(&context, req, res.get());
    return res;
}
