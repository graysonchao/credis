// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: v3election.proto

#include "v3election.pb.h"
#include "v3election.grpc.pb.h"

#include <grpc++/impl/codegen/async_stream.h>
#include <grpc++/impl/codegen/sync_stream.h>
#include <gmock/gmock.h>
namespace v3electionpb {

class MockElectionStub : public Election::StubInterface {
 public:
  MOCK_METHOD3(Campaign, ::grpc::Status(::grpc::ClientContext* context, const ::v3electionpb::CampaignRequest& request, ::v3electionpb::CampaignResponse* response));
  MOCK_METHOD3(AsyncCampaignRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::CampaignResponse>*(::grpc::ClientContext* context, const ::v3electionpb::CampaignRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncCampaignRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::CampaignResponse>*(::grpc::ClientContext* context, const ::v3electionpb::CampaignRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(Proclaim, ::grpc::Status(::grpc::ClientContext* context, const ::v3electionpb::ProclaimRequest& request, ::v3electionpb::ProclaimResponse* response));
  MOCK_METHOD3(AsyncProclaimRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::ProclaimResponse>*(::grpc::ClientContext* context, const ::v3electionpb::ProclaimRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncProclaimRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::ProclaimResponse>*(::grpc::ClientContext* context, const ::v3electionpb::ProclaimRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(Leader, ::grpc::Status(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request, ::v3electionpb::LeaderResponse* response));
  MOCK_METHOD3(AsyncLeaderRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::LeaderResponse>*(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncLeaderRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::LeaderResponse>*(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD2(ObserveRaw, ::grpc::ClientReaderInterface< ::v3electionpb::LeaderResponse>*(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request));
  MOCK_METHOD4(AsyncObserveRaw, ::grpc::ClientAsyncReaderInterface< ::v3electionpb::LeaderResponse>*(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request, ::grpc::CompletionQueue* cq, void* tag));
  MOCK_METHOD3(PrepareAsyncObserveRaw, ::grpc::ClientAsyncReaderInterface< ::v3electionpb::LeaderResponse>*(::grpc::ClientContext* context, const ::v3electionpb::LeaderRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(Resign, ::grpc::Status(::grpc::ClientContext* context, const ::v3electionpb::ResignRequest& request, ::v3electionpb::ResignResponse* response));
  MOCK_METHOD3(AsyncResignRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::ResignResponse>*(::grpc::ClientContext* context, const ::v3electionpb::ResignRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncResignRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3electionpb::ResignResponse>*(::grpc::ClientContext* context, const ::v3electionpb::ResignRequest& request, ::grpc::CompletionQueue* cq));
};

} // namespace v3electionpb

