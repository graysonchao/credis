#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "etcd/etcd.h"

using namespace ::testing;

namespace {

    TEST(TestChain, Test1) {
    auto kv_stub = std::make_shared<MockKVStub>();
    auto watch_stub = std::make_shared<MockWatchStub>();
    auto lease_stub = std::make_shared<MockLeaseStub>();
    auto lock_stub = std::make_shared<MockLockStub>();
    auto mock_client = std::unique_ptr<etcd::EtcdClient>(
        new etcd::EtcdClient(kv_stub, watch_stub, lease_stub, lock_stub)
    );

    auto state = std::unordered_map<std::string, std::string> {};
    Chain c("testapp", "testchain", state);
    c.AddMember("0");
    c.AddMember("1");
}


  // TEST(TestEtcdClient, TestPut) {
  // auto kv_stub = std::make_shared<MockKVStub>();
  // auto watch_stub = std::make_shared<MockWatchStub>();
  // auto lease_stub = std::make_shared<MockLeaseStub>();
  // auto lock_stub  = std::make_shared<MockLockStub>();
  // PutRequest expected;
  // expected.set_key("foo");
  // expected.set_value("bar");
  // expected.set_lease(0);
  // expected.set_prev_kv(false);
  // expected.set_ignore_value(false);
  // expected.set_ignore_value(false);
  // EXPECT_CALL(
  //     *kv_stub,
  //     Put(_, EqualsProtobufMessage(expected), _)
  // );
  // auto client = etcd::EtcdClient(kv_stub, watch_stub, lease_stub, lock_stub);
  // client.Put("foo", "bar", 0, false, false, false);
  // }

  // TEST(TestCoordinator, Test)
  };

}
