//
// Created by Grayson Chao on 2/17/18.
//

#include <future>
#include <utility>
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

etcd::EtcdClient::EtcdClient(
    std::shared_ptr<KV::StubInterface> kv_stub,
    std::shared_ptr<Watch::StubInterface> watch_stub,
    std::shared_ptr<Lease::StubInterface> lease_stub,
    std::shared_ptr<Lock::StubInterface> lock_stub
) :
    kv_stub_(std::move(kv_stub)),
    watch_stub_(watch_stub),
    lease_stub_(lease_stub),
    lock_stub_(lock_stub)
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
  req.set_ignore_lease(ignore_lease);

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

std::unique_ptr<UnlockResponse> etcd::EtcdClient::Unlock(
    std::string lock_key
) {
  grpc::Status status;
  grpc::ClientContext context;
  UnlockRequest req;
  auto res = std::unique_ptr<UnlockResponse>(new UnlockResponse());

  req.set_key(lock_key);
  status = lock_stub_->Unlock(&context, req, res.get());
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
 * Initiate a watch stream for the given key, range end, etc. by creating a stream and sending a WatchCreateRequest.
 * The caller gets back a pointer to a stream that already has a WatchResponse buffered.
 * @param key
 * @param range_end
 * @param start_revision Revision to start watching from - existing events after this revision will stream immediately
 * @param progress_notify Set to "true" if etcd should periodically inform us of a recent revision (for fast recovery).
 * @param filters A list of event types that etcd should filter out and not send.
 * @param prev_kv Set to "true" if etcd should report the previous value of changed keys.
 * @return
 */
std::unique_ptr<grpc::ClientReaderWriterInterface<WatchRequest, WatchResponse>>
etcd::EtcdClient::WatchCreate(
    const std::string &key,
    const std::string &range_end,
    const int64_t start_revision = -1,
    const bool progress_notify = true,
    const std::vector<WatchCreateRequest_FilterType> filters = {},
    const bool prev_kv = false
) {
  grpc::Status status;
  // TODO gchao: Does this leak memory? It's not clear if the GRPC stub frees the context. Can't find any docs.
  auto context = new grpc::ClientContext();
  auto wcr = new WatchCreateRequest();
  wcr->set_key(key);
  wcr->set_range_end(range_end);
  if (start_revision >= 0) {
    wcr->set_start_revision(start_revision);
  }
  wcr->set_progress_notify(progress_notify);
  for (WatchCreateRequest_FilterType filter : filters) {
    wcr->add_filters(filter);
  }

  WatchRequest wrapper;
  wrapper.set_allocated_create_request(wcr);
  auto watch_stream = watch_stub_->Watch(context);
  watch_stream->Write(wrapper);
  return watch_stream;
}

/**
 * Cancel the given watch. Note that whatever stream was being used to monitor the connection must be closed separately.
 * @param watch_id
 */
void etcd::EtcdClient::WatchCancel(const int64_t watch_id) {
  grpc::Status status;
  grpc::ClientContext context;
  auto wcr = std::unique_ptr<WatchCancelRequest>(new WatchCancelRequest());
  wcr->set_watch_id(watch_id);

  WatchRequest wrapper;
  wrapper.set_allocated_cancel_request(wcr.get());
  auto watch_stream = watch_stub_->Watch(&context);
  watch_stream->Write(wrapper);
  watch_stream->WritesDone();
}

/**
 * Build a Compare object that checks if a key exists.
 * @param key the key to check.
 * @return
 */
std::unique_ptr<Compare> etcd::txn::BuildKeyExistsComparison(
    const std::string &key
) {
  auto comparison = std::unique_ptr<Compare>(new Compare());
  comparison->set_result(Compare_CompareResult_GREATER);
  comparison->set_target(Compare_CompareTarget_CREATE);
  comparison->set_key(key);
  comparison->set_create_revision(0);
  return comparison;
}

std::unique_ptr<RequestOp> etcd::txn::BuildPutRequest(
    const std::string &key,
    const std::string &value
) {
  auto pr = new PutRequest();
  pr->set_key(key);
  pr->set_value(value);

  auto request_op = std::unique_ptr<RequestOp>(new RequestOp());
  request_op->set_allocated_request_put(pr);
  return request_op;
}

/**
 * Build a RequestOp to get a single key.
 * @return
 */
std::unique_ptr<RequestOp> etcd::txn::BuildGetRequest(
  const std::string key
) {
  auto rr = new RangeRequest();
  rr->set_key(key);
  rr->set_range_end("");

  auto request_op = std::unique_ptr<RequestOp>(new RequestOp());
  request_op->set_allocated_request_range(rr);
  return request_op;
}
