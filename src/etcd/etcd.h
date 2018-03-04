//
// A wrapper for the etcd gRPC API (v3).
//

#ifndef CREDIS_ETCD_H
#define CREDIS_ETCD_H

#include <grpc++/grpc++.h>
#include "protos/src/rpc.pb.h"
#include "protos/src/rpc.grpc.pb.h"
#include "protos/src/v3lock.pb.h"
#include "protos/src/v3lock.grpc.pb.h"

using namespace etcdserverpb;
using namespace v3lockpb;
using namespace mvccpb;

namespace etcd {

    class ClientInterface {
    public:
        virtual ~ClientInterface() = default;
        virtual std::unique_ptr<PutResponse> Put(
            std::string key,
            std::string value,
            int64_t lease = 0,
            bool prev_key = false,
            bool ignore_value = false,
            bool ignore_lease = false
        ) = 0;

        virtual std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            int64_t revision = -1
        ) = 0;
        virtual std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            int64_t revision,
            grpc::Status &status
        ) = 0;

        virtual std::unique_ptr<grpc::ClientReaderWriterInterface<WatchRequest, WatchResponse>>
        WatchCreate(
            const std::string &key,
            const std::string &range_end,
            int64_t start_revision,
            bool progress_notify,
            std::vector<WatchCreateRequest_FilterType> filters,
            bool prev_kv
        ) = 0;
        virtual void WatchCancel(int64_t watch_id) = 0;

        virtual std::unique_ptr<LeaseGrantResponse> LeaseGrant(
            int64_t requested_ttl,
            int64_t requested_id = 0
        ) = 0;
        virtual std::unique_ptr<LeaseKeepAliveResponse> LeaseKeepAlive(int64_t id) = 0;

        virtual std::unique_ptr<LockResponse> Lock(std::string lock_name, int64_t lease_id) = 0;
        virtual std::unique_ptr<UnlockResponse> Unlock(std::string lock_key) = 0;

        virtual std::unique_ptr<TxnResponse> Transaction(
            const std::vector<Compare> &comparisons,
            const std::vector<RequestOp> &success_ops,
            const std::vector<RequestOp> &failure_ops
        ) = 0;
    };

    class Client : public ClientInterface {
    public:
        explicit Client(std::shared_ptr<grpc::ChannelInterface> channel);
        Client(
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
        ) override;

        std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            int64_t revision = -1
        ) override;
        std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            int64_t revision,
            grpc::Status &status
        ) override;

        std::unique_ptr<grpc::ClientReaderWriterInterface<WatchRequest, WatchResponse>>
          WatchCreate(
            const std::string &key,
            const std::string &range_end,
            int64_t start_revision,
            bool progress_notify,
            std::vector<WatchCreateRequest_FilterType> filters,
            bool prev_kv
        ) override;

        void WatchCancel(int64_t watch_id) override;

        std::unique_ptr<LeaseGrantResponse> LeaseGrant(
            int64_t requested_ttl,
            int64_t requested_id = 0
        ) override;

        std::unique_ptr<LeaseKeepAliveResponse> LeaseKeepAlive(int64_t id) override;

        std::unique_ptr<LockResponse> Lock(std::string lock_name, int64_t lease_id) override;

        std::unique_ptr<UnlockResponse> Unlock(std::string lock_key) override;

        std::unique_ptr<TxnResponse> Transaction(
            const std::vector<Compare> &comparisons,
            const std::vector<RequestOp> &success_ops,
            const std::vector<RequestOp> &failure_ops
        ) override;

    private:
        // Shared for mocking.
        std::shared_ptr<KV::StubInterface> kv_stub_;
        std::shared_ptr<Watch::StubInterface> watch_stub_;
        std::shared_ptr<Lease::StubInterface> lease_stub_;
        std::shared_ptr<Lock::StubInterface> lock_stub_;
    };

    namespace util {
        // Helper functions for transactions
        std::unique_ptr<Compare> BuildKeyExistsComparison(const std::string &key);
        std::unique_ptr<Compare> BuildKeyNotExistsComparison(const std::string &key);
        std::unique_ptr<RequestOp> BuildPutRequest(
            const std::string &key,
            const std::string &value
        );
        std::unique_ptr<RequestOp> BuildRangeRequest(
            const std::string &key,
            const std::string &range_end
        );
        std::unique_ptr<RequestOp> BuildPutRequest(const std::string &key, const std::string &value);
        std::unique_ptr<RequestOp> BuildGetRequest(const std::string &key);
        std::unique_ptr<RequestOp> BuildDeleteRequest(const std::string &key);
        std::string RangePrefix(const std::string &key);
    }
}



#endif //CREDIS_ETCD_H
