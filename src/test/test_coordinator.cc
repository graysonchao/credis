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
      grpc::Status Transaction(const TxnRequest &req, TxnResponse* res) {
        auto hdr = new ResponseHeader();
        hdr->set_revision(42);
        *res = TxnResponse();
        res->set_allocated_header(hdr);
        res->set_succeeded(true);
        Transaction_(req, res);
        return grpc::Status(grpc::StatusCode::OK, "fake status");
      }
      MOCK_METHOD2(Put,
                   grpc::Status(const PutRequest &req, PutResponse *res));
      MOCK_METHOD2(Range,
                   grpc::Status(const RangeRequest &request, RangeResponse *response));
      MOCK_METHOD1(MakeWatchStream,
                   etcd::WatchStreamPtr(const WatchRequest& req));
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
      MOCK_METHOD2(Transaction_,
                   grpc::Status(const TxnRequest &req, TxnResponse *res));
    };

    class CoordinatorTest : public ::testing::Test {
    protected:
        virtual void SetUp() {
        }
    };
};
