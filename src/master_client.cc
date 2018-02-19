#include <string>

#include "master_client.h"
#include "glog/logging.h"

using namespace etcd;


Status MasterClient::Connect(const std::string& address, int port) {
  auto channel = grpc::CreateChannel("127.0.0.1:22379", grpc::InsecureChannelCredentials());
  etcd_client_ = std::shared_ptr<EtcdClient>(new EtcdClient(channel));
  return Status::OK();
}

const std::string MasterClient::WatermarkKey(Watermark w) const {
  std::string watermark_key = (w == MasterClient::Watermark::kSnCkpt) ? "_sn_ckpt" : "_sn_flushed";
  return key_prefix_ + watermark_key;
}

Status MasterClient::GetWatermark(Watermark w, int64_t* val) const {
  std::string key = WatermarkKey(w);
  std::unique_ptr<RangeResponse> response = etcd_client_->Range(key, "");

  if (response->kvs_size() > 0) {
    auto kvs = response->kvs();
    const KeyValue result = kvs.Get(0);
    *val = std::stol(result.value());
    LOG(INFO) << "(etcd) GET " << WatermarkKey(w) << ": " << result.value();
    return Status::OK();
  } else {
    switch (w) {
      case Watermark::kSnCkpt:
        *val = kSnCkptInit;
            break;
      case Watermark::kSnFlushed:
        *val = kSnFlushedInit;
            break;
    }
    return Status::OK();
  }
}

Status MasterClient::SetWatermark(Watermark w, int64_t new_val) {
  std::unique_ptr<PutResponse> response = etcd_client_->Put(
      WatermarkKey(w),
      std::to_string(new_val),
      0,
      true
  );
  std::string prev_value = response->prev_kv().value();

  LOG(INFO) << "(etcd) PUT " << WatermarkKey(w) << ": " << new_val
            << " (was " << prev_value << ")";

  return Status::OK();
}
