//
// A wrapper for the etcd gRPC API (v3).
//

#ifndef CREDIS_ETCD_H
#define CREDIS_ETCD_H

#include<grpc++/grpc++.h>
#include "protos/src/rpc.pb.h"
#include "protos/src/rpc.grpc.pb.h"

using namespace etcdserverpb;
using namespace mvccpb;

namespace etcd {

    class EtcdClient {
    public:
        EtcdClient(std::shared_ptr<grpc::ChannelInterface> channel);

        std::unique_ptr<PutResponse> Put(
            grpc::Status &status,
            std::string key,
            std::string value,
            // TODO: move default params to etcd.cc so that includer sees the default
            int64_t lease = 0,
            bool prev_key = false,
            bool ignore_value = false,
            bool ignore_lease = false
        );

        std::unique_ptr<PutResponse> Put(
                std::string key,
                std::string value,
                int64_t lease = 0,
                bool prev_key = false,
                bool ignore_value = false,
                bool ignore_lease = false
        );

        std::unique_ptr<RangeResponse> Range(
                grpc::Status &status,
                const std::string &key,
                const std::string &range_end,
                const int64_t revision = -1
        );

        std::unique_ptr<RangeResponse> Range(
            const std::string &key,
            const std::string &range_end,
            const int64_t revision = -1
        );

    private:
        std::unique_ptr<KV::Stub> kv_stub_;
        std::unique_ptr<Watch::Stub> watch_stub_;
        std::unique_ptr<Lease::Stub> lease_stub_;
    };
}



#endif //CREDIS_ETCD_H
