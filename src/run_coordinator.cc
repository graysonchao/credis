#include <chrono>
#include "glog/logging.h"
#include "grpcpp/grpcpp.h"
#include "etcd/etcd.h"
#include "coordinator.h"
//
// Created by Grayson Chao on 3/2/18.
//

int main(int argc, char* argv[]) {
  std::string address = "127.0.0.1";
  int port = 2379;
  auto channel = grpc::CreateChannel(
      address + ":" + std::to_string(port),
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
      << "Managing chain " << chain_id << " at " << address << ":" << port;
  grpc::Status status = c.ManageChain(chain_id);
  if (status.ok()) {
    LOG(FATAL) << status.error_message();
  }
}
