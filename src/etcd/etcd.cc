//
// Created by Grayson Chao on 2/17/18.
//

#include <future>
#include <utility>
#include <leveldb/include/leveldb/status.h>
#include "glog/logging.h"
#include "etcd.h"

using namespace etcdserverpb;
using namespace v3lockpb;
using namespace mvccpb;

using etcd::WatchStreamPtr;

etcd::Client::Client(std::shared_ptr<grpc::ChannelInterface> channel)
    : kv_stub_(KV::NewStub(channel)),
      watch_stub_(Watch::NewStub(channel)),
      lease_stub_(Lease::NewStub(channel)),
      lock_stub_(Lock::NewStub(channel)) {}

etcd::Client::Client(
    std::shared_ptr<KV::StubInterface> kv_stub,
    std::shared_ptr<Watch::StubInterface> watch_stub,
    std::shared_ptr<Lease::StubInterface> lease_stub,
    std::shared_ptr<Lock::StubInterface> lock_stub
) :
    kv_stub_(kv_stub),
    watch_stub_(watch_stub),
    lease_stub_(lease_stub),
    lock_stub_(lock_stub) {}

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
grpc::Status etcd::Client::Put(const PutRequest& req, PutResponse* res) {
  grpc::ClientContext context;
  return kv_stub_->Put(&context, req, res);
}

/**
 * Query etcd for a key or a range of keys.
 * Returns a unique_ptr to a RangeResponse.
 * @param status a grpc::Status that will be overwritten with the reply's status.
 * @param key
 * @param range_end
 * @param revision (latest by default)
 * @return
 */
grpc::Status etcd::Client::Range(const RangeRequest& req, RangeResponse* res) {
  grpc::ClientContext context;
  return kv_stub_->Range(&context, req, res);
}

grpc::Status etcd::Client::LeaseGrant(const LeaseGrantRequest& req,
                                      LeaseGrantResponse* res) {
  grpc::ClientContext context;
  return lease_stub_->LeaseGrant(&context, req, res);
}

grpc::Status etcd::Client::LeaseKeepAlive(
    const LeaseKeepAliveRequest& req,
    LeaseKeepAliveResponse* res
) {
  grpc::ClientContext context;
  auto stream = lease_stub_->LeaseKeepAlive(&context);
  stream->Write(req);
  stream->WritesDone();
  stream->Read(res);
  return stream->Finish();
}

grpc::Status etcd::Client::Lock(const LockRequest& req, LockResponse* res) {
  grpc::ClientContext context;
  return lock_stub_->Lock(&context, req, res);
}

grpc::Status etcd::Client::Unlock(const UnlockRequest& req,
                                  UnlockResponse* res) {
  grpc::ClientContext context;
  return lock_stub_->Unlock(&context, req, res);
}

grpc::Status etcd::Client::Transaction(const TxnRequest& req,
                                       TxnResponse* res) {
  grpc::ClientContext context;
  return kv_stub_->Txn(&context, req, res);
}

// Initiate a watch stream for the given key, range end, etc. by creating a stream and sending a WatchCreateRequest.
// The caller gets back a pointer to a stream that already has a WatchResponse buffered.
WatchStreamPtr etcd::Client::MakeWatchStream(const WatchRequest& req) {
  // TODO gchao: Does this leak memory? It's not clear if the GRPC stub frees the context. Can't find any docs.
  auto context = new grpc::ClientContext();
  auto watch_stream = watch_stub_->Watch(context);
  watch_stream->Write(req);
  return watch_stream;
}

void etcd::Client::WatchCancel(int64_t watch_id) {
  grpc::ClientContext context;
  auto watch_stream = watch_stub_->Watch(&context);
  auto wcr = new WatchCancelRequest();
  wcr->set_watch_id(watch_id);
  WatchRequest req;
  req.set_allocated_cancel_request(wcr);
  watch_stream->Write(req);
  watch_stream->WritesDone();
}

// Utility functions, mostly for working with transactions

void etcd::util::MakeKeyExistsCompare(
    const std::string& key,
    Compare* compare
) {
  compare->set_key(key);
  compare->set_result(Compare_CompareResult_GREATER);
  compare->set_target(Compare_CompareTarget_CREATE);
  compare->set_create_revision(0);
}

void etcd::util::MakeKeyNotExistsCompare(
    const std::string& key,
    Compare* compare
) {
  compare->set_key(key);
  compare->set_result(Compare_CompareResult_LESS);
  compare->set_target(Compare_CompareTarget_CREATE);
  compare->set_create_revision(1);
}

/**
 * Convenience method to construct a RangeRequest that gets all keys where KEY is a prefix.
 * This exists because of etcd's questionable decision to make (int)key+1 a magic value for the range end:
 * If you GetRange(KEY, to_string ((int)KEY+1), you are really getting all keys prefixed with KEY.
 * Usage: EtcdClient::Range(key, EtcdClient::RangePrefix(key))
 * @return
 */
std::string etcd::util::RangePrefix(const std::string& key) {
  // If the last char is \xff the prefix "wraps" (https://coreos.com/etcd/docs/latest/learning/api.html#key-value-api)
  const auto last_usable_char_pos = key.find_last_not_of((char) '\xFF');
  CHECK(last_usable_char_pos != 0 && last_usable_char_pos != std::string::npos)
  << "Can't take the prefix of a key string whose only bytes are 0xFF.";
  // substr takes a length, but find_last_not_of returns a position, so we add 1.
  auto prefix = key.substr(0, last_usable_char_pos + 1);
  prefix.back() += 1;
  return prefix;
}


// Given a function returning a grpc::Status, repeatedly call the function
// until timeout elapses or the returned status == OK.
// WARNING: makes no attempt to ensure that elapsed_ms follows wall time.
grpc::Status etcd::util::ExponentialBackoff(std::function<grpc::Status()> job,
                                            etcd::util::BackoffOpts opts) {
  int elapsed_ms = 0;
  while (true) {
    auto status = job();
    std::this_thread::sleep_for(std::chrono::milliseconds(opts.interval_ms));
    elapsed_ms += opts.interval_ms;
    opts.interval_ms *= opts.multiplier;
    if (elapsed_ms > opts.timeout_ms || status.ok()) {
      return status;
    }
  }
}
grpc::Status etcd::util::ExponentialBackoff(std::function<grpc::Status()> job) {
  return etcd::util::ExponentialBackoff(job, etcd::util::BackoffOpts());
}
