// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: v3lock.proto

#include "v3lock.pb.h"
#include "v3lock.grpc.pb.h"

#include <grpc++/impl/codegen/async_stream.h>
#include <grpc++/impl/codegen/sync_stream.h>
#include <gmock/gmock.h>
namespace v3lockpb {

class MockLockStub : public Lock::StubInterface {
 public:
  MOCK_METHOD3(Lock, ::grpc::Status(::grpc::ClientContext* context, const ::v3lockpb::LockRequest& request, ::v3lockpb::LockResponse* response));
  MOCK_METHOD3(AsyncLockRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3lockpb::LockResponse>*(::grpc::ClientContext* context, const ::v3lockpb::LockRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncLockRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3lockpb::LockResponse>*(::grpc::ClientContext* context, const ::v3lockpb::LockRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(Unlock, ::grpc::Status(::grpc::ClientContext* context, const ::v3lockpb::UnlockRequest& request, ::v3lockpb::UnlockResponse* response));
  MOCK_METHOD3(AsyncUnlockRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3lockpb::UnlockResponse>*(::grpc::ClientContext* context, const ::v3lockpb::UnlockRequest& request, ::grpc::CompletionQueue* cq));
  MOCK_METHOD3(PrepareAsyncUnlockRaw, ::grpc::ClientAsyncResponseReaderInterface< ::v3lockpb::UnlockResponse>*(::grpc::ClientContext* context, const ::v3lockpb::UnlockRequest& request, ::grpc::CompletionQueue* cq));
};

} // namespace v3lockpb

