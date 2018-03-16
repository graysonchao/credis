#include <chrono>
#include <memory>
#include <thread>
#include "crow/crow_all.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "etcd/etcd.h"
#include "etcd_master.h"

DEFINE_string(etcd_host, "127.0.0.1", "etcd hostname");
DEFINE_string(chain_id, "", "chain id to join");
DEFINE_int32(etcd_port, 2379, "etcd port");
DEFINE_int32(http_api_port, 8080, "port to serve service discovery info on");

void run_master(std::shared_ptr<grpc::ChannelInterface> channel_) {
  std::shared_ptr<grpc::ChannelInterface> channel = channel_;
  auto etcd = std::unique_ptr<etcd::ClientInterface>(new etcd::Client(channel));
  EtcdMaster c(std::move(etcd));
  LOG(INFO)
      << "Managing chain " << FLAGS_chain_id
      << " at " << FLAGS_etcd_host << ":" << FLAGS_etcd_port;
  grpc::Status status = c.ManageChain(FLAGS_chain_id);
  if (!status.ok()) {
    LOG(FATAL) << status.error_message();
  }
}

// Run a simple HTTP server that returns the connection info for the head/tail.
// e.g. GET 127.0.0.1:8080/head -> {"address": ___, "port": ___, "role": ___}
void run_http_api(std::shared_ptr<grpc::ChannelInterface> channel_) {
  std::shared_ptr<grpc::ChannelInterface> channel = channel_;
  crow::SimpleApp app;
  app.loglevel(crow::LogLevel::Warning);
  CROW_ROUTE(app, "/head")
      ([channel_](){
        if (channel_->GetState(false) == GRPC_CHANNEL_SHUTDOWN) {
          return crow::response(503);
        }
        etcd::Client etcd(channel_);
        auto chain = EtcdMaster::ReadChain(etcd, FLAGS_chain_id);
        auto head = chain.Head();
        crow::json::wvalue response;
        response["address"] = chain.Head().heartbeat.address;
        response["port"] = chain.Head().heartbeat.port;
        response["role"] = chain.Head().heartbeat.config.role;
        return crow::response(response);
      });

  CROW_ROUTE(app, "/tail")([channel_](){
    if (channel_->GetState(false) == GRPC_CHANNEL_SHUTDOWN) {
      return crow::response(503);
    }
    etcd::Client etcd(channel_);
    auto chain = EtcdMaster::ReadChain(etcd, FLAGS_chain_id);
    crow::json::wvalue response;
    response["address"] = chain.Tail().heartbeat.address;
    response["port"] = chain.Tail().heartbeat.port;
    response["role"] = chain.Tail().heartbeat.config.role;
    return crow::response(response);
  });
  LOG(INFO) << "Starting HTTP API at 0.0.0.0:" << FLAGS_http_api_port;
  app.port(FLAGS_http_api_port).run();
}

int main(int argc, char* argv[]) {
  gflags::SetUsageMessage("You must provide a chain ID.");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  if (FLAGS_chain_id.empty()) {
    gflags::ShowUsageWithFlagsRestrict(argv[0], "run_etcd_master.cc");
    exit(1);
  }
  auto channel = grpc::CreateChannel(
      FLAGS_etcd_host + ":" + std::to_string(FLAGS_etcd_port),
      grpc::InsecureChannelCredentials()
  );
  std::thread http_api(run_http_api, channel);
  run_master(channel);
  http_api.join();
}
