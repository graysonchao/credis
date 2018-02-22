//
// A wrapper for the etcd gRPC API (v3).
//

#ifndef CREDIS_ETCD_H
#define CREDIS_ETCD_H

#include<grpc++/grpc++.h>
#include "protos/src/rpc.pb.h"
#include "protos/src/rpc.grpc.pb.h"
#include "protos/src/v3lock.pb.h"
#include "protos/src/v3lock.grpc.pb.h"

using namespace etcdserverpb;
using namespace v3lockpb;
using namespace mvccpb;

namespace etcd {

    class EtcdClient {
    public:
        EtcdClient(std::shared_ptr<grpc::ChannelInterface> channel);

        EtcdClient(
            std::shared_ptr<KV::StubInterface> kv_stub,
            std::shared_ptr<Watch::StubInterface> watch_stub,
            std::shared_ptr<Lease::StubInterface> lease_stub,
            std::shared_ptr<Lock::StubInterface> lock_stub
        );

        std::unique_ptr<PutResponse> Put(
            std::string key,
            std::string value,
            // TODO: move default params to etcd.cc so that includer sees the default
            int64_t lease = 0,
            bool prev_key = false,
            bool ignore_value = false,
            bool ignore_lease = false
        );

        std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            int64_t revision = -1
        );

        std::unique_ptr<LeaseGrantResponse> LeaseGrant(
            int64_t requested_ttl,
            int64_t requested_id = 0
        );

        std::unique_ptr<LeaseKeepAliveResponse> LeaseKeepAlive(
            int64_t id
        );

        std::unique_ptr<LockResponse> Lock(
            std::string name,
            int64_t lease_id
        );

        std::unique_ptr<TxnResponse> Transaction(
            std::vector<Compare>& comparisons,
            std::vector<RequestOp>& success_ops,
            std::vector<RequestOp>& failure_ops
        );

        std::unique_ptr<Compare> CompareKeyExists(
            const std::string &key
        );

    private:
        // Shared for mocking.
        std::shared_ptr<KV::StubInterface> kv_stub_;
        std::shared_ptr<Watch::StubInterface> watch_stub_;
        std::shared_ptr<Lease::StubInterface> lease_stub_;
        std::shared_ptr<Lock::StubInterface> lock_stub_;
    };
}



#endif //CREDIS_ETCD_H
