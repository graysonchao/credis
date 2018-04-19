#include <chrono>
#include <fstream>
#include <random>
#include <thread>
#include <unordered_set>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "client.h"
#include "timer.h"

// Command-line flags
DEFINE_int32(num_ops, 1000000, "Number of operations to do");
DEFINE_int32(num_nodes, 1, "Number of chain nodes to use");
DEFINE_double(write_ratio, 0.5, "Write ratio (0.0 - 1.0)");
DEFINE_string(write_address, "127.0.0.1", "Address of write instance");
DEFINE_int32(write_port, 6370, "Port of write instance");
DEFINE_string(ack_address, "127.0.0.1", "Address of read/ack instance");
DEFINE_int32(ack_port,
             FLAGS_write_port + FLAGS_num_nodes - 1,
             "Port of read/ack instance");
DEFINE_string(stats_file, "/dev/null", "File to write detailed timing stats");

// TODO(zongheng): timeout should be using exponential backoff and/or some
// randomization; this is critical in distributed settings (e.g., multiple
// processes running this same program) to avoid catastrophic failures.

// To launch with 2 servers:
//
//   pkill -f redis-server; ./setup.sh 2; make -j;
//   ./src/credis_seqput_bench 2
//
// If "2" is omitted in the above, by default 1 server is used.

aeEventLoop* loop = aeCreateEventLoop(64);
int writes_completed = 0;
int reads_completed = 0;
Timer reads_timer, writes_timer;
std::string last_issued_read_key;
// Randomness.
std::default_random_engine re;

const long long kRetryTimerMillisecs = 100;  // For ae's timer.
double last_unacked_timestamp = -1;
int64_t last_unacked_seqnum = -1;

redisAsyncContext* write_context = nullptr;
redisAsyncContext* read_context = nullptr;

// Client's bookkeeping for seqnums.
std::unordered_set<int64_t> assigned_seqnums;
std::unordered_set<int64_t> acked_seqnums;

// Clients also need UniqueID support.
// TODO(zongheng): implement this properly via uuid, currently it's pid.
const std::string client_id = std::to_string(getpid());

// Forward declaration.
void SeqPutCallback(redisAsyncContext*, void*, void*);
void SeqGetCallback(redisAsyncContext*, void*, void*);
void AsyncPut(bool);
void AsyncGet();

// Launch a GET or PUT, depending on "write_ratio".
void AsyncRandomCommand() {
  static std::uniform_real_distribution<double> unif(0.0, 1.0);
  const double r = unif(re);
  if (r < FLAGS_write_ratio || writes_completed == 0) {
    AsyncPut(/*is_retry=*/false);
  } else {
    AsyncGet();
  }
}

void OnCompleteLaunchNext(Timer* timer, int* cnt, int other_cnt) {
  // Sometimes an ACK comes back late, just ignore if we're done.
  if (*cnt + other_cnt >= FLAGS_num_ops) return;
  ++(*cnt);
  timer->TimeOpEnd(*cnt);
  if (*cnt + other_cnt == FLAGS_num_ops) {
    aeStop(loop);
    return;
  }
  // Launch next pair.
  AsyncRandomCommand();
}

// This callback gets fired whenever the store assigns a seqnum for a Put
// request.
void SeqPutCallback(redisAsyncContext* write_context,  // != ack_context.
                    void* r,
                    void*) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  const int64_t assigned_seqnum = reply->integer;
  // LOG(INFO) << "SeqPutCallback " << assigned_seqnum;
  auto it = acked_seqnums.find(assigned_seqnum);
  if (it != acked_seqnums.end()) {
    acked_seqnums.erase(it);
    last_unacked_timestamp = -1;
    last_unacked_seqnum = -1;
    OnCompleteLaunchNext(&writes_timer, &writes_completed, reads_completed);
  } else {
    last_unacked_seqnum = assigned_seqnum;
    assigned_seqnums.insert(assigned_seqnum);
  }
}

// Put(n -> n), for n == writes_completed.
void AsyncPut(bool is_retry) {
  const std::string data = std::to_string(writes_completed);
  // LOG(INFO) << "PUT " << data;
  if (!is_retry) last_unacked_timestamp = writes_timer.TimeOpBegin();
  const int status = redisAsyncCommand(
      write_context, &SeqPutCallback,
      /*privdata=*/NULL, "MEMBER.PUT %b %b %b", data.data(), data.size(),
      data.data(), data.size(), client_id.data(), client_id.size());
  CHECK(status == REDIS_OK);
}

// Get(i), for a random i in [0, writes_completed).
void AsyncGet() {
  CHECK(writes_completed > 0);
  std::uniform_int_distribution<> unif_int(0, writes_completed - 1);
  const int r = unif_int(re);
  // LOG(INFO) << "random int " << r << " writes_completed " <<
  // writes_completed;
  const std::string query = std::to_string(r);
  last_issued_read_key = query;

  reads_timer.TimeOpBegin();
  // LOG(INFO) << "GET " << query;
  const int status = redisAsyncCommand(
      read_context, reinterpret_cast<redisCallbackFn*>(&SeqGetCallback),
      /*privdata=*/NULL, "GET %b", query.data(), query.size());
  CHECK(status == REDIS_OK);
}

void SeqGetCallback(redisAsyncContext* /*context*/,
                    void* r,
                    void* /*privdata*/) {
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  // LOG(INFO) << "reply type " << reply->type << "; issued get "
  //           << last_issued_read_key;
  const std::string actual = std::string(reply->str, reply->len);
  CHECK(last_issued_read_key == actual)
      << "; expected " << last_issued_read_key << " actual " << actual;
  OnCompleteLaunchNext(&reads_timer, &reads_completed, writes_completed);
}

// This gets fired whenever an ACK from the store comes back.
void SeqPutAckCallback(redisAsyncContext* ack_context,  // != write_context.
                       void* r,
                       void* privdata) {
  /* Replies to the SUBSCRIBE command have 3 elements. There are two
   * possibilities. Either the reply is the initial acknowledgment of the
   * subscribe command, or it is a message. If it is the initial acknowledgment,
   * then
   *     - reply->element[0]->str is "subscribe"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is null.
   * If it is an actual message, then
   *     - reply->element[0]->str is "message"
   *     - reply->element[1]->str is the name of the channel
   *     - reply->emement[2]->str is the contents of the message.
   */
  const redisReply* reply = reinterpret_cast<redisReply*>(r);
  const redisReply* message_type = reply->element[0];

  if (reply == nullptr) {
    LOG(INFO) << "reply nullptr: " << ack_context->errstr;
  }

  // NOTE(zongheng): this is a hack.
  // if (strcmp(message_type->str, "message") == 0) {
  if (reply->element[2]->str == nullptr) {
    LOG(INFO) << getpid() << " subscribed";
    LOG(INFO) << getpid() << " chan: " << reply->element[1]->str;
    return;
  }
  const int64_t received_sn = std::stoi(reply->element[2]->str);

  auto it = assigned_seqnums.find(received_sn);
  if (it == assigned_seqnums.end()) {
    // LOG(INFO) << "seqnum acked " << received_sn;
    acked_seqnums.insert(received_sn);
    return;
  }
  // Otherwise, found & act on this ACK.
  // LOG(INFO) << "seqnum acked " << received_sn;
  assigned_seqnums.erase(it);
  last_unacked_timestamp = -1;
  last_unacked_seqnum = -1;
  OnCompleteLaunchNext(&writes_timer, &writes_completed, reads_completed);
}

// const double kRetryTimeoutMicrosecs = 500000;
const double kRetryTimeoutMicrosecs = 5 * 1e6;  // 5 sec
int RetryPutTimer(aeEventLoop* loop, long long /*timer_id*/, void*) {
  const double now_us = writes_timer.NowMicrosecs();
  const double diff = now_us - last_unacked_timestamp;
  if (last_unacked_timestamp > 0 && diff > kRetryTimeoutMicrosecs) {
    LOG(INFO) << "Retrying PUT " << writes_completed;
    LOG(INFO) << " time diff (us) " << diff << "; last_unacked_seqnum "
              << last_unacked_seqnum;
    // If the ACK comes back later, we should ignore it.
    if (last_unacked_seqnum >= 0) {
      auto it = assigned_seqnums.find(last_unacked_seqnum);
      CHECK(it != assigned_seqnums.end());
      assigned_seqnums.erase(it);
      last_unacked_seqnum = -1;
    }
    AsyncPut(/*is_retry=*/true);
  }
  return kRetryTimerMillisecs;  // Reset ae's timer to 0.
}

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_write_ratio = FLAGS_write_ratio;
  std::string write_address = FLAGS_write_address;
  const int write_port = FLAGS_write_port;
  std::string ack_address = FLAGS_ack_address;
  const int ack_port = FLAGS_ack_port;
  LOG(INFO) << "write ratio: " << FLAGS_write_ratio;
  LOG(INFO) << "write server: " << write_address;
  LOG(INFO) << "write port: " << write_port;
  LOG(INFO) << "ack server: " << ack_address;
  LOG(INFO) << "ack port: " << ack_port;

  RedisClient client;
  RedisAddress write_addr = {write_address, write_port};
  RedisAddress ack_addr = {ack_address, ack_port};
  CHECK(client.Connect(write_addr, ack_addr).ok());
  CHECK(client.AttachToEventLoop(loop).ok());
  CHECK(client
            .RegisterAckCallback(
                static_cast<redisCallbackFn*>(&SeqPutAckCallback))
            .ok());
  write_context = client.write_context();
  read_context = client.read_context();

  // Timings related.
  reads_timer.ExpectOps(FLAGS_num_ops);
  writes_timer.ExpectOps(FLAGS_num_ops);

  aeCreateTimeEvent(loop, /*milliseconds=*/kRetryTimerMillisecs, &RetryPutTimer,
                    NULL, NULL);

  std::this_thread::sleep_for(std::chrono::seconds(1));
  LOG(INFO) << "starting bench";
  auto start = std::chrono::system_clock::now();

  // Start with first query, and each callback will launch the next.
  AsyncRandomCommand();

  // LOG(INFO) << "start loop";
  aeMain(loop);
  // LOG(INFO) << "end loop";

  auto end = std::chrono::system_clock::now();
  CHECK(writes_completed + reads_completed == FLAGS_num_ops);
  LOG(INFO) << "ending bench";
  const int64_t latency_us =
      std::chrono::duration_cast<std::chrono::microseconds>(end - start)
          .count();

  // Timings related.
  Timer merged = Timer::Merge(reads_timer, writes_timer);
  double composite_mean = 0, composite_std = 0;
  merged.Stats(&composite_mean, &composite_std);
  double reads_mean = 0, reads_std = 0;
  reads_timer.Stats(&reads_mean, &reads_std);
  double writes_mean = 0, writes_std = 0;
  writes_timer.Stats(&writes_mean, &writes_std);

  LOG(INFO) << "throughput " << FLAGS_num_ops * 1e6 / latency_us
            << " ops/s, total duration (ms) " << latency_us / 1e3 << ", num "
            << FLAGS_num_ops << ", write_ratio " << FLAGS_write_ratio;
  LOG(INFO) << "reads_thput "
            << reads_completed * 1e6 / (reads_mean * reads_completed)
            << " ops/s, total duration(ms) "
            << (reads_mean * reads_completed) / 1e3 << ", num "
            << reads_completed;
  LOG(INFO) << "writes_thput "
            << writes_completed * 1e6 / (writes_mean * writes_completed)
            << " ops/s, total duration(ms) "
            << (writes_mean * writes_completed) / 1e3 << ", num "
            << writes_completed;

  LOG(INFO) << "latency (us) mean " << composite_mean << " std "
            << composite_std;
  LOG(INFO) << "reads_lat (us) mean " << reads_mean << " std " << reads_std;
  LOG(INFO) << "writes_lat (us) mean " << writes_mean << " std " << writes_std;

  {
    auto detailed_write_stats = writes_timer.TimeTable();
    auto detailed_read_stats = reads_timer.TimeTable();
    std::ofstream ofs(FLAGS_stats_file);
    ofs << "type,start,end" << std::endl;
    ofs.setf(std::ios_base::fixed);
    for (auto times : detailed_write_stats) {
      ofs << "write," << times.first << "," << times.second << std::endl;
    }
    for (auto times : detailed_read_stats) {
      ofs << "read," << times.first << "," << times.second << std::endl;
    }
  }

  return 0;
}
