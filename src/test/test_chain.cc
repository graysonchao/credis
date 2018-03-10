#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "etcd/etcd.h"
#include "chain.h"

using namespace ::testing;

using chain::MemberKey;
using chain::MemberConfig;
using chain::MemberHeartbeat;
using chain::kRoleSingleton;
using chain::kRoleHead;
using chain::kRoleTail;
using chain::kRoleUninitialized;
using chain::kNoMember;

namespace {

static chain::Member MakeLivingMember(int64_t id) {
  chain::Member m1(id);
  m1.config = MemberConfig(kRoleUninitialized, kNoMember, kNoMember);
  m1.heartbeat = MemberHeartbeat("127.0.0.1",
                                 1337,
                                 m1.config.role,
                                 m1.config.prev,
                                 m1.config.next);
  return m1;
}

static chain::Member MakeDeadMember(int64_t id) {
  chain::Member m1(id);
  m1.config = MemberConfig(kRoleUninitialized, kNoMember, kNoMember);
  return m1;
}

TEST(ChainTest, TestAddMember_WasEmpty) {
  chain::Chain chain("unit_tests", "test_id");

  chain::Member m1 = MakeLivingMember(1);
  chain.AddMember(m1);

  ASSERT_EQ(chain.Head().id, 1);
  ASSERT_EQ(chain.Tail().id, 1);
  ASSERT_EQ(chain.members.size(), 1);

  ASSERT_EQ(chain.Head().config.role, kRoleSingleton);
  ASSERT_EQ(chain.Head().config.prev, kNoMember);
  ASSERT_EQ(chain.Head().config.next, kNoMember);
}

TEST(ChainTest, TestAddMember_WasSingleton) {
  chain::Chain chain("unit_tests", "test_id");

  chain::Member m1 = MakeLivingMember(1);
  chain.AddMember(m1);

  chain::Member m2 = MakeLivingMember(2);
  chain.AddMember(m2);

  ASSERT_EQ(chain.Head().id, 1);
  ASSERT_EQ(chain.Tail().id, 2);
  ASSERT_EQ(chain.members.size(), 2);

  ASSERT_EQ(chain.Head().config.role, kRoleHead);
  ASSERT_EQ(chain.Head().config.prev, kNoMember);
  ASSERT_EQ(chain.Head().config.next, 2);

  ASSERT_EQ(chain.Tail().config.role, kRoleTail);
  ASSERT_EQ(chain.Tail().config.prev, 1);
  ASSERT_EQ(chain.Tail().config.next, kNoMember);
}

TEST(ChainTest, TestAddMember_WasNotSingleton) {
  chain::Chain chain("unit_tests", "test_id");

  chain::Member m1 = MakeLivingMember(1);
  chain.AddMember(m1);

  chain::Member m2 = MakeLivingMember(2);
  chain.AddMember(m2);

  chain::Member m3 = MakeLivingMember(3);
  chain.AddMember(m3);

  ASSERT_EQ(chain.members.size(), 3);
  ASSERT_EQ(chain.Head().id, 1);
  ASSERT_EQ(chain.Tail().id, 3);

  ASSERT_EQ(chain.Head().config.role, kRoleHead);
  ASSERT_EQ(chain.Head().config.prev, kNoMember);
  ASSERT_EQ(chain.Head().config.next, 2);

  ASSERT_EQ(chain.Tail().config.role, kRoleTail);
  ASSERT_EQ(chain.Tail().config.prev, 2);
  ASSERT_EQ(chain.Tail().config.next, kNoMember);
}

TEST(TestChain, TestRemoveMember_RemoveHead) {
  chain::Chain chain("unit_tests", "test_id");
  chain.AddMember(MakeLivingMember(1));
  chain.AddMember(MakeLivingMember(2));
  chain.AddMember(MakeLivingMember(3));
  chain.RemoveMember(1);

  EXPECT_FALSE(chain.HasMember(1));
  EXPECT_TRUE(chain.HasMember(2));
  EXPECT_TRUE(chain.HasMember(3));

  EXPECT_EQ(chain.Head().id, 2);
  EXPECT_EQ(chain.Tail().id, 3);
  EXPECT_EQ(chain.members[2].config.prev, kNoMember);
  EXPECT_EQ(chain.members[2].config.next, 3);
  EXPECT_EQ(chain.members[3].config.prev, 2);
}

TEST(TestChain, TestRemoveMember_RemoveMiddle) {
  chain::Chain chain("unit_tests", "test_id");
  chain.AddMember(MakeLivingMember(1));
  chain.AddMember(MakeLivingMember(2));
  chain.AddMember(MakeLivingMember(3));
  chain.RemoveMember(2);

  EXPECT_TRUE(chain.HasMember(1));
  EXPECT_FALSE(chain.HasMember(2));
  EXPECT_TRUE(chain.HasMember(3));

  EXPECT_EQ(chain.members[1].config.next, 3);
  EXPECT_EQ(chain.members[3].config.prev, 1);
}

TEST(TestChain, TestRemoveMember_RemoveTail) {
  chain::Chain chain("unit_tests", "test_id");
  chain.AddMember(MakeLivingMember(1));
  chain.AddMember(MakeLivingMember(2));
  chain.AddMember(MakeLivingMember(3));
  chain.RemoveMember(3);

  EXPECT_TRUE(chain.HasMember(1));
  EXPECT_TRUE(chain.HasMember(2));
  EXPECT_FALSE(chain.HasMember(3));

  EXPECT_EQ(chain.members[1].config.next, 2);
  EXPECT_EQ(chain.members[2].config.prev, 1);
  EXPECT_EQ(chain.members[2].config.next, kNoMember);
}

TEST(TestChain, TestRemoveMember_Nonexistent) {
  chain::Chain chain("unit_tests", "test_id");
  chain.AddMember(MakeLivingMember(1));
  chain.AddMember(MakeLivingMember(2));
  chain.AddMember(MakeLivingMember(3));
  chain.RemoveMember(4);

  EXPECT_TRUE(chain.HasMember(1));
  EXPECT_TRUE(chain.HasMember(2));
  EXPECT_TRUE(chain.HasMember(3));

  EXPECT_EQ(chain.members[1].config.next, 2);

  EXPECT_EQ(chain.members[2].config.prev, 1);
  EXPECT_EQ(chain.members[2].config.next, 3);

  EXPECT_EQ(chain.members[3].config.prev, 2);
  EXPECT_EQ(chain.members[3].config.next, kNoMember);

  EXPECT_EQ(chain.Tail().id, 3);
}

}
