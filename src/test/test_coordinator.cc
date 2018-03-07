#include <memory>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "etcd/etcd.h"
#include "src/coordinator.h"
#include "google/protobuf/util/message_differencer.h"

using namespace ::testing;
using namespace etcdserverpb;
using json = nlohmann::json;
using ::chain::MemberConfig;
using ::chain::MemberHeartbeat;

namespace {

    MATCHER_P(PBEq, expected, "") {
      return google::protobuf::util::MessageDifferencer::Equals(expected, arg);
    }

    class MockClient : public ::etcd::ClientInterface {
     public:
      MOCK_METHOD2(Put,
                   grpc::Status(const PutRequest &req, PutResponse *res));
      MOCK_METHOD2(Range,
                   grpc::Status(const RangeRequest &request, RangeResponse *response));
      MOCK_METHOD2(MakeWatchStream,
                   etcd::WatchStreamPtr(const WatchRequest& req, WatchResponse* res));
      MOCK_METHOD1(WatchCancel,
                   void(int64_t watch_id));
      MOCK_METHOD2(LeaseGrant,
                   grpc::Status(const LeaseGrantRequest &req, LeaseGrantResponse *res));
      MOCK_METHOD2(LeaseKeepAlive,
                   grpc::Status(const LeaseKeepAliveRequest &req, LeaseKeepAliveResponse *res));
      MOCK_METHOD2(Lock,
                   grpc::Status(const LockRequest &req, LockResponse *res));
      MOCK_METHOD2(Unlock,
                   grpc::Status(const UnlockRequest &req, UnlockResponse *res));
      MOCK_METHOD2(Transaction,
                   grpc::Status(const TxnRequest &req, TxnResponse *res));
    };

    class CoordinatorTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
          ::chain::Member m1(1, MemberConfig(chain::kRoleHead, chain::kNoMember, 2), MemberHeartbeat());
          ::chain::Member m2(2, MemberConfig(chain::kRoleMiddle, 1, 3), MemberHeartbeat());
          ::chain::Member m3(3, MemberConfig(chain::kRoleTail, 2, chain::kNoMember), MemberHeartbeat());
          m1_config_json_ = m1.config->ToJSON();
          m2_config_json_ = m2.config->ToJSON();
          m3_config_json_ = m3.config->ToJSON();
          chain_of_three_ = std::unique_ptr<chain::Chain>(
              new ::chain::Chain("test_app", "chain_of_3", {
                  {m1_hb_key_, m1.heartbeat->ToJSON()},
                  {m2_hb_key_, m2.heartbeat->ToJSON()},
                  {m3_hb_key_, m3.heartbeat->ToJSON()},
                  {m1_config_key_, m1.config->ToJSON()},
                  {m2_config_key_, m2.config->ToJSON()},
                  {m3_config_key_, m3.config->ToJSON()}
              }));

          ::chain::Member s1(42, MemberConfig(chain::kRoleSingleton, chain::kNoMember, chain::kNoMember), MemberHeartbeat());
          s1_config_json_ = s1.config->ToJSON();
          chain_of_one_ = std::unique_ptr<chain::Chain>(
              new ::chain::Chain("test_app", "chain_of_1", {
                  {s1_hb_key_, s1.heartbeat->ToJSON()},
                  {s1_config_key_, s1.config->ToJSON()},
              }));
        }

        TxnResponse* SuccessTx() {
          auto tx = new TxnResponse();
          auto hdr = new ResponseHeader();
          tx->set_allocated_header(hdr);
          tx->set_succeeded(true);
          return tx;
        }


        std::string m1_hb_key_ = MemberKey("test_app", "chain_of_3", 1, "hb").ToString();
        std::string m2_hb_key_ = MemberKey("test_app", "chain_of_3", 2, "hb").ToString();
        std::string m3_hb_key_ = MemberKey("test_app", "chain_of_3", 3, "hb").ToString();
        std::string m1_config_key_ = MemberKey("test_app", "chain_of_3", 1, "config").ToString();
        std::string m2_config_key_ = MemberKey("test_app", "chain_of_3", 2, "config").ToString();
        std::string m3_config_key_ = MemberKey("test_app", "chain_of_3", 3, "config").ToString();
        std::string m1_config_json_;
        std::string m2_config_json_;
        std::string m3_config_json_;

        std::string s1_hb_key_ = MemberKey("test_app", "chain_of_1", 1, "hb").ToString();
        std::string s1_config_key_ = MemberKey("test_app", "chain_of_1", 1, "config").ToString();
        std::string s1_config_json_;

        std::unique_ptr<::chain::Chain> chain_of_one_;
        std::unique_ptr<::chain::Chain> chain_of_three_;
    };

    TEST_F(CoordinatorTest, TestHeartbeatExpired_Head) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      c.HandleHeartbeatExpired(*chain_of_three_, 1);
      EXPECT_EQ(chain_of_three_->head_id,  2);
      EXPECT_EQ(chain_of_three_->tail_id, 3);
      EXPECT_EQ(chain_of_three_->members.size(), 2);
      EXPECT_EQ(chain_of_three_->members[2].config->role, chain::kRoleHead);
      EXPECT_EQ(chain_of_three_->members[2].config->prev, chain::kNoMember);
      EXPECT_EQ(chain_of_three_->members[2].config->next, 3);
      EXPECT_EQ(chain_of_three_->members[3].config->role, chain::kRoleTail);
      EXPECT_EQ(chain_of_three_->members[3].config->prev, 2);
      EXPECT_EQ(chain_of_three_->members[3].config->next, chain::kNoMember);
    }

    TEST_F(CoordinatorTest, TestHeartbeatExpired_Tail) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      c.HandleHeartbeatExpired(*chain_of_three_, 3);
      EXPECT_EQ(chain_of_three_->head_id,  1);
      EXPECT_EQ(chain_of_three_->tail_id, 2);
      EXPECT_EQ(chain_of_three_->members.size(), 2);
      EXPECT_EQ(chain_of_three_->members[1].config->role, chain::kRoleHead);
      EXPECT_EQ(chain_of_three_->members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(chain_of_three_->members[1].config->next, 2);
      EXPECT_EQ(chain_of_three_->members[2].config->role, chain::kRoleTail);
      EXPECT_EQ(chain_of_three_->members[2].config->prev, 1);
      EXPECT_EQ(chain_of_three_->members[2].config->next, chain::kNoMember);
    }

    TEST_F(CoordinatorTest, TestHeartbeatExpired_Middle) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      c.HandleHeartbeatExpired(*chain_of_three_, 2);
      EXPECT_EQ(chain_of_three_->head_id,  1);
      EXPECT_EQ(chain_of_three_->tail_id, 3);
      EXPECT_EQ(chain_of_three_->members.size(), 2);
      EXPECT_EQ(chain_of_three_->members[1].config->role, chain::kRoleHead);
      EXPECT_EQ(chain_of_three_->members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(chain_of_three_->members[1].config->next, 3);
      EXPECT_EQ(chain_of_three_->members[3].config->role, chain::kRoleTail);
      EXPECT_EQ(chain_of_three_->members[3].config->prev, 1);
      EXPECT_EQ(chain_of_three_->members[3].config->next, chain::kNoMember);
    }

    TEST_F(CoordinatorTest, TestHeartbeatExpired_Singleton) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      chain::Chain c1("test", "test", {});
      c1.AddMember(1, MemberHeartbeat().ToJSON());
      c.HandleHeartbeatExpired(c1, 1);
      EXPECT_TRUE(c1.members.empty());
    }

    TEST_F(CoordinatorTest, TestHeartbeatExpired_ExpiredNodeNeverJoined) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      c.HandleHeartbeatExpired(*chain_of_three_, -37);
      EXPECT_EQ(chain_of_three_->members.size(), 3);
    }

    TEST_F(CoordinatorTest, TestHandleNodeJoin_ChainWasNotSingleton) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      MemberHeartbeat new_hb(
          json {
              {"prev", chain::kNoMember},
              {"next", chain::kNoMember},
              {"role", chain::kRoleUninitialized},
              {"address", "127.0.0.1"},
              {"port", 31337}
          }.dump()
      );
      chain::Chain c3("test", "test", {});
      c3.AddMember(1, MemberHeartbeat().ToJSON());
      c3.AddMember(2, MemberHeartbeat().ToJSON());
      c3.AddMember(3, MemberHeartbeat().ToJSON());
      c.HandleNodeJoin(c3, 4, new_hb.ToJSON());
      EXPECT_EQ(c3.head_id,  1);
      EXPECT_EQ(c3.tail_id, 4);
      EXPECT_EQ(c3.members.size(), 4);
      EXPECT_EQ(c3.members[1].config->role, chain::kRoleHead);
      EXPECT_EQ(c3.members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(c3.members[1].config->next, 2);
      EXPECT_EQ(c3.members[2].config->role, chain::kRoleMiddle);
      EXPECT_EQ(c3.members[2].config->prev, 1);
      EXPECT_EQ(c3.members[2].config->next, 3);
      EXPECT_EQ(c3.members[3].config->role, chain::kRoleMiddle);
      EXPECT_EQ(c3.members[3].config->prev, 2);
      EXPECT_EQ(c3.members[3].config->next, 4);
      EXPECT_EQ(c3.members[4].config->role, chain::kRoleTail);
      EXPECT_EQ(c3.members[4].config->prev, 3);
      EXPECT_EQ(c3.members[4].config->next, chain::kNoMember);
    }

    TEST_F(CoordinatorTest, TestHandleNodeJoin_ChainWasSingleton) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      MemberHeartbeat new_hb(
          json {
              {"prev", chain::kNoMember},
              {"next", chain::kNoMember},
              {"role", chain::kRoleUninitialized},
              {"address", "127.0.0.1"},
              {"port", 31337}
          }.dump()
      );
      auto chain = ::chain::Chain("test_app", "chain_of_1", {});
      chain.AddMember(1, MemberHeartbeat().ToJSON());
      EXPECT_EQ(chain.members[1].config->role, chain::kRoleSingleton);
      EXPECT_EQ(chain.members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(chain.members[1].config->next, chain::kNoMember);

      c.HandleNodeJoin(chain, 2, new_hb.ToJSON());
      EXPECT_EQ(chain.head_id, 1);
      EXPECT_EQ(chain.tail_id, 2);
      EXPECT_EQ(chain.members.size(), 2);
      EXPECT_EQ(chain.members[1].config->role, chain::kRoleHead);
      EXPECT_EQ(chain.members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(chain.members[1].config->next, 2);
      EXPECT_EQ(chain.members[2].config->role, chain::kRoleTail);
      EXPECT_EQ(chain.members[2].config->prev, 1);
      EXPECT_EQ(chain.members[2].config->next, chain::kNoMember);
    }

    TEST_F(CoordinatorTest, TestHandleNodeJoin_ChainWasEmpty) {
      auto mock_client = std::unique_ptr<etcd::ClientInterface>(new MockClient());
      auto raw_client_ptr = (MockClient*) mock_client.get();
      auto tx = SuccessTx();
      Coordinator c(std::move(mock_client));
      MemberHeartbeat new_hb(
          json {
              {"prev", chain::kNoMember},
              {"next", chain::kNoMember},
              {"role", chain::kRoleUninitialized},
              {"address", "127.0.0.1"},
              {"port", 31337}
          }.dump()
      );
      Chain empty_chain("test_app", "test_id", {});
      c.HandleNodeJoin(empty_chain, 1, new_hb.ToJSON());
      EXPECT_EQ(empty_chain.members.size(), 1);
      EXPECT_EQ(empty_chain.members[1].config->role, chain::kRoleSingleton);
      EXPECT_EQ(empty_chain.members[1].config->prev, chain::kNoMember);
      EXPECT_EQ(empty_chain.members[1].config->next, chain::kNoMember);
    }
};
