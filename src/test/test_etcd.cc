#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "etcd/etcd.h"
#include "google/protobuf/util/message_differencer.h"
#include "protos/src/rpc.pb.h"
#include "protos/src/rpc_mock.grpc.pb.h"
#include "protos/src/v3lock.pb.h"
#include "protos/src/v3lock_mock.grpc.pb.h"

using namespace ::testing;

namespace {

    MATCHER_P(EqualsProtobufMessage, expected, "") {
      return google::protobuf::util::MessageDifferencer::Equals(expected, arg);
    }

    TEST(TestEtcdClient, TestPut) {
      auto kv_stub = std::make_shared<MockKVStub>();
      auto watch_stub = std::make_shared<MockWatchStub>();
      auto lease_stub = std::make_shared<MockLeaseStub>();
      auto lock_stub  = std::make_shared<MockLockStub>();
      PutRequest expected;
      expected.set_key("foo");
      expected.set_value("bar");
      expected.set_lease(0);
      expected.set_prev_kv(false);
      expected.set_ignore_value(false);
      expected.set_ignore_value(false);
      EXPECT_CALL(
          *kv_stub,
          Put(_, EqualsProtobufMessage(expected), _)
      );
      auto client = etcd::EtcdClient(kv_stub, watch_stub, lease_stub, lock_stub);
      client.Put("foo", "bar", 0, false, false, false);
    }

    TEST(TestEtcdClient, TestRange) {
      auto kv_stub = std::make_shared<MockKVStub>();
      auto watch_stub = std::make_shared<MockWatchStub>();
      auto lease_stub = std::make_shared<MockLeaseStub>();
      auto lock_stub  = std::make_shared<MockLockStub>();
      RangeRequest expected;
      expected.set_key("foo");
      expected.set_range_end("bar");
      expected.set_revision(0);
      EXPECT_CALL(
          *kv_stub,
          Range(_, EqualsProtobufMessage(expected), _)
      );
      auto client = etcd::EtcdClient(kv_stub, watch_stub, lease_stub, lock_stub);
      client.Range("foo", "bar", 0);
    }

    TEST(TestEtcdClient, TestLeaseGrant) {
      auto kv_stub = std::make_shared<MockKVStub>();
      auto watch_stub = std::make_shared<MockWatchStub>();
      auto lease_stub = std::make_shared<MockLeaseStub>();
      auto lock_stub  = std::make_shared<MockLockStub>();
      LeaseGrantRequest expected;
      expected.set_ttl(0);
      expected.set_id(0);
      EXPECT_CALL(
          *lease_stub,
          LeaseGrant(_, EqualsProtobufMessage(expected), _)
      );
      auto client = etcd::EtcdClient(kv_stub, watch_stub, lease_stub, lock_stub);
      client.LeaseGrant(0, 0);
    }
};
