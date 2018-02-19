//
// Created by Grayson Chao on 2/17/18.
//

#include <future>
#include <leveldb/include/leveldb/status.h>
#include <glog/logging.h>
#include "etcd.h"

using namespace etcdserverpb;
using namespace v3lockpb;
using namespace mvccpb;

etcd::EtcdClient::EtcdClient(std::shared_ptr<grpc::ChannelInterface> channel)
    : kv_stub_(KV::NewStub(channel)),
      watch_stub_(Watch::NewStub(channel)),
      lease_stub_(Lease::NewStub(channel)),
      lock_stub_(Lock::NewStub(channel))
{}

/**
 * Put a value to a key in etcd
 * @param status
 * @param key
 * @param value
 * @param lease
 * @param prev_key
 * @param ignore_value
 * @param ignore_lease
 * @return
 */
std::unique_ptr<PutResponse> etcd::EtcdClient::Put(
    const std::string key,
    const std::string value,
    const int64_t lease,
    const bool prev_key,
    const bool ignore_value,
    const bool ignore_lease
) {
  grpc::Status status;
  grpc::ClientContext context;
  PutRequest req;
  req.set_key(key);
  req.set_value(value);
  req.set_lease(lease);
  req.set_prev_kv(prev_key);
  req.set_ignore_value(ignore_value);

  auto res = std::unique_ptr<PutResponse>(new PutResponse());
  status = kv_stub_->Put(&context, req, res.get());
  return res;
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
    const std::string &key,
    const std::string &range_end,
    const int64_t revision
) {
  grpc::Status status;
  grpc::ClientContext context;
  RangeRequest req;
  req.set_key(key);
  req.set_range_end(range_end);
  req.set_revision(revision);

  auto res = std::unique_ptr<RangeResponse>(new RangeResponse());
  status = kv_stub_->Range(&context, req, res.get());
  return res;
}

std::unique_ptr<LeaseGrantResponse> etcd::EtcdClient::LeaseGrant(
    int64_t requested_ttl,
    int64_t requested_id
) {
  grpc::Status status;
  grpc::ClientContext context;
  LeaseGrantRequest req;
  auto res = std::unique_ptr<LeaseGrantResponse>(new LeaseGrantResponse());

  req.set_ttl(requested_ttl);
  req.set_id(requested_id);
  status = lease_stub_->LeaseGrant(&context, req, res.get());
  return res;
}

std::unique_ptr<LeaseKeepAliveResponse> etcd::EtcdClient::LeaseKeepAlive(
    int64_t id
) {
  grpc::Status status;
  grpc::ClientContext context;
  LeaseKeepAliveRequest req;
  auto res = std::unique_ptr<LeaseKeepAliveResponse>(new LeaseKeepAliveResponse);

  auto stream = lease_stub_->LeaseKeepAlive(&context);
  req.set_id(id);
  stream->Write(req);
  stream->WritesDone();

  stream->Read(res.get());
  status = stream->Finish();
  return res;
}

std::unique_ptr<LockResponse> etcd::EtcdClient::Lock(
    std::string name,
    int64_t lease_id
) {
  grpc::Status status;
  grpc::ClientContext context;
  LockRequest req;
  auto res = std::unique_ptr<LockResponse>(new LockResponse());

  req.set_name(name);
  req.set_lease(lease_id);
  status = lock_stub_->Lock(&context, req, res.get());
  return res;
}

/**
 * Guard some sequence of operations with an etcd transaction.
 * @param comparisons A list of conditions to be evaluated.
 * @param success_ops Operations to perform if all conditions are true.
 * @param failure_ops Operations to perform if all conditions are false.
 * @return A response indicating success and the results of ops that were performed.
 */
std::unique_ptr<TxnResponse> etcd::EtcdClient::Transaction(
    std::vector<Compare>& comparisons,
    std::vector<RequestOp>& success_ops,
    std::vector<RequestOp>& failure_ops
) {
  grpc::Status status;
  grpc::ClientContext context;
  TxnRequest req;
  auto res = std::unique_ptr<TxnResponse>(new TxnResponse());

  //TODO gchao: construct this request without so much copying?
  for (Compare comparison : comparisons) {
    Compare* added = req.add_compare();
    added->CopyFrom(comparison);
  }

  for (RequestOp success_op : success_ops) {
    RequestOp* added = req.add_success();
    added->CopyFrom(success_op);
  }

  for (RequestOp failure_op : failure_ops) {
    RequestOp* added = req.add_failure();
    added->CopyFrom(failure_op);
  }

  status = kv_stub_->Txn(&context, req, res.get());
  return res;
}

/**
 * Build a Compare object that checks if a key exists.
 * @param key the key to check.
 * @return
 */
std::unique_ptr<Compare> etcd::EtcdClient::CompareKeyExists(
    const std::string &key
) {
  auto comparison = std::unique_ptr<Compare>(new Compare());
  comparison->set_result(Compare_CompareResult_GREATER);
  comparison->set_target(Compare_CompareTarget_CREATE);
  comparison->set_key(key);
  comparison->set_create_revision(0);
  return comparison;
}
