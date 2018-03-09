//
// A wrapper for the etcd gRPC API (v3).
//

#ifndef CREDIS_ETCD_H
#define CREDIS_ETCD_H

#include <grpcpp/grpcpp.h>
#include "rpc.pb.h"
#include "rpc.grpc.pb.h"
#include "v3lock.pb.h"
#include "v3lock.grpc.pb.h"

using namespace etcdserverpb;
using namespace v3lockpb;
using namespace mvccpb;

using std::string;

namespace etcd {

typedef std::unique_ptr<
    grpc::ClientReaderWriterInterface<WatchRequest, WatchResponse>>
    WatchStreamPtr;

// A thin client wrapper for the etcd3 grpc interface.
class ClientInterface {
 public:
  ClientInterface() {}

  virtual ~ClientInterface() = default;

  virtual grpc::Status Put(const PutRequest &req, PutResponse *res) = 0;

  // Get a range of keys, which can also be a single key or the set of all keys
  // matching a prefix.
  virtual grpc::Status Range(const RangeRequest &request,
                             RangeResponse *response) = 0;

  // Create a watch stream, which is a bidirectional GRPC stream where the
  // client receives all change events to the requested keys.
  virtual WatchStreamPtr MakeWatchStream(const WatchRequest& req,
                                         WatchResponse* res) = 0;

  virtual void WatchCancel(int64_t watch_id) = 0;

  // Request a lease, which is a session with etcd kept alive by
  // LeaseKeepAlive requests. It can be associated with keys and locks to
  // delete or release them when the session times out, respectively.
  virtual grpc::Status LeaseGrant(const LeaseGrantRequest &req,
                                  LeaseGrantResponse *res) = 0;

  virtual grpc::Status LeaseKeepAlive(const LeaseKeepAliveRequest &req,
                                      LeaseKeepAliveResponse *res) = 0;

  virtual grpc::Status Lock(const LockRequest &req, LockResponse *res) = 0;

  virtual grpc::Status Unlock(const UnlockRequest &req,
                              UnlockResponse *res) = 0;

  // Perform a transaction, which is a set of boolean predicates and two sets
  // of operations: one set to do if the predicates are all true, and one set
  // to do otherwise.
  virtual grpc::Status Transaction(const TxnRequest &req, TxnResponse *res) = 0;
};

class Client : public ClientInterface {
 public:
  explicit Client(std::shared_ptr<grpc::ChannelInterface> channel);

  Client(std::shared_ptr<KV::StubInterface> kv_stub,
         std::shared_ptr<Watch::StubInterface> watch_stub,
         std::shared_ptr<Lease::StubInterface> lease_stub,
         std::shared_ptr<Lock::StubInterface> lock_stub);

  grpc::Status Put(const PutRequest &request, PutResponse *response) override;

  grpc::Status Range(const RangeRequest& request,
                     RangeResponse* response) override;

  WatchStreamPtr MakeWatchStream(const WatchRequest& req,
                                 WatchResponse* res) override;

  void WatchCancel(int64_t watch_id) override;

  grpc::Status LeaseGrant(const LeaseGrantRequest &req,
                          LeaseGrantResponse *res) override;

  grpc::Status LeaseKeepAlive(const LeaseKeepAliveRequest &req,
                              LeaseKeepAliveResponse *res) override;

  grpc::Status Lock(const LockRequest &req, LockResponse *res) override;

  grpc::Status Unlock(const UnlockRequest &req, UnlockResponse *res) override;

  grpc::Status Transaction(const TxnRequest &req, TxnResponse *res) override;

 private:
  // Shared for mocking.
  std::shared_ptr<KV::StubInterface> kv_stub_;
  std::shared_ptr<Watch::StubInterface> watch_stub_;
  std::shared_ptr<Lease::StubInterface> lease_stub_;
  std::shared_ptr<Lock::StubInterface> lock_stub_;
};

namespace util {
// Helper functions for transactions
void MakeKeyExistsCompare(const std::string &key, Compare *compare);

void MakeKeyNotExistsCompare(const std::string &key, Compare *compare);

void AllocatePutRequest(const std::string &key, const std::string &value,
                        RequestOp *requestOp);

std::unique_ptr<RequestOp> BuildRangeRequest(
    const std::string &key,
    const std::string &range_end
);

std::unique_ptr<RequestOp>
BuildPutRequest(const std::string &key, const std::string &value);

std::unique_ptr<RequestOp> BuildGetRequest(const std::string &key);

std::unique_ptr<RequestOp> BuildDeleteRequest(const std::string &key);

std::string RangePrefix(const std::string &key);

// Repeatedly retry calling JOB, waiting an exponentially increasing amount
// of time between tries. Stops if JOB returns an OK status, or if TIMEOUT_MS
// elapses. Note: Not guaranteed to be accurate in terms of wall time.
grpc::Status ExponentialBackoff(
    std::function<grpc::Status()> job,
    int interval_ms,
    int timeout_ms,
    float multiplier
);
}

}

#endif //CREDIS_ETCD_H
