#include <chrono>
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "etcd/etcd.h"
#include "coordinator.h"
//
// Created by Grayson Chao on 3/2/18.
//

DEFINE_string(address, "127.0.0.1", "address of etcd");
DEFINE_int32(port, 2379, "listen port of etcd");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  auto channel = grpc::CreateChannel(
      FLAGS_address + ":" + std::to_string(FLAGS_port),
      grpc::InsecureChannelCredentials()
  );
  auto etcd = std::unique_ptr<etcd::ClientInterface>(new etcd::Client(channel));
  Coordinator c(std::move(etcd));
  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " CHAIN_ID\n";
    exit(1);
  }
  std::string chain_id(argv[1]);
  LOG(INFO)
      << "Managing chain " << chain_id
      << " at " << FLAGS_address << ":" << FLAGS_port;
  grpc::Status status = c.ManageChain(chain_id);
  if (status.ok()) {
    LOG(FATAL) << status.error_message();
  }
}
