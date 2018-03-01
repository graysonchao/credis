#ifndef CREDIS_COORDINATOR_H_
#define CREDIS_COORDINATOR_H_

#include <iostream>

#include "nlohmann/json.hpp"
#include "glog/logging.h"
#include "hiredis/hiredis.h"
#include "etcd/etcd.h"
#include "chain.h"

using namespace chain;

class Coordinator {
public:
    struct Options {
        bool auto_add_new_members;
    };
    explicit Coordinator(std::unique_ptr<etcd::ClientInterface> etcd);
    Coordinator(std::unique_ptr<etcd::ClientInterface> etcd, Options options);
    std::unique_ptr<grpc::Status> Connect(const std::string& address, int port);
    void ManageChain(std::string chain_id);
    void HandleHeartbeatExpired(
        Chain &chain,
        const std::string &failed_id
    );
    void ListenForChanges(
        const std::string& chain_id,
        const RangeResponse &initial_state
    );
    void FlushChain(const chain::Chain &chain);
    void HandleNodeJoin(
        Chain &chain,
        const std::string &new_id,
        const std::string &new_hb_str
    );

private:
    static const std::string kKeyPrefix;
    bool IsSystemKey(const std::string &key);

    Options options_;
    std::shared_ptr<grpc::Channel> channel_;
    std::unique_ptr<etcd::ClientInterface> etcd_;
    std::unique_ptr<
        grpc::ClientReaderWriterInterface<
            WatchRequest, WatchResponse>> changes_;
    // TODO: multiple chain support
    // std::string chain_ids;
};

#endif  // CREDIS_COORDINATOR_H_
