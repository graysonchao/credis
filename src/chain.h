//
// Created by Grayson Chao on 3/1/18.
//

#ifndef CREDIS_CHAIN_H
#define CREDIS_CHAIN_H

#include <string>
#include <map>

#include "etcd3/include/etcd3.h"

namespace chain {
const std::string kRoleHead = "head";
const std::string kRoleTail = "tail";
const std::string kRoleMiddle = "middle";
const std::string kRoleSingleton = "singleton";
const std::string kRoleUninitialized = "uninitialized";
const std::string kKeyTypeHeartbeat = "hb";
const std::string kKeyTypeConfig = "config";
const int64_t kNoMember = -1;

class MemberKey {
 public:
  explicit MemberKey(const std::string &_key_str);
  MemberKey(
      std::string prefix,
      int64_t member_id,
      std::string type
  );
  std::string ToString() const;

  std::string prefix;
  int64_t id;
  std::string type;
};

class Config {
 public:
  Config();
  explicit Config(const etcd3::pb::KeyValue& kv);
  explicit Config(
      MemberKey key,
      std::string role,
      int64_t prev,
      int64_t next
  );
  bool operator==(const Config& other);
  bool operator!=(const Config& other);
  std::string ToJSON() const;
  bool IsInitialized();
  MemberKey key;
  std::string role;
  int64_t prev;
  int64_t next;
};

class Heartbeat {
 public:
  Heartbeat();
  explicit Heartbeat(const etcd3::pb::KeyValue& kv);
  explicit Heartbeat(
      MemberKey key,
      std::string address,
      int port,
      std::string role,
      int64_t prev,
      int64_t next
  );
  std::string ToJSON() const;

  bool IsInitialized();
  MemberKey key;
  Config config;
  std::string address;
  int port;
};

class Member {
 public:
  Member();
  explicit Member(int64_t id);
  bool HasHeartbeat();
  bool HasConfig();
  int64_t id;
  Config config;
  Heartbeat heartbeat;
};

/**
 * The Chain class represents a CR chain.
 * It provides a semantic interface (Head(), Tail(), etc.) and the ability
 * to serialize a full set of intended configs.
 */
class Chain {
 public:
  explicit Chain(std::string prefix);
  // Give
  void AddMember(Member member);
  void AddHeartbeat(Heartbeat heartbeat);
  void AddConfig(Config config);
  void RemoveMember(int64_t id);
  bool HasMember(int64_t id);

  Member Head();
  Member Tail();

  std::vector<std::pair<std::string, std::string>> SerializedState();
  std::string ConfigKey(int64_t member_id) const;
  std::string MemberDirectory(int64_t member_id) const;

  std::map<int64_t, Member> members;
  std::string prefix;
  int64_t head_id;
  int64_t tail_id;

 private:
  void SetNext(int64_t member_id, int64_t next_id);
  void SetPrev(int64_t member_id, int64_t prev_id);
  void SetRole(int64_t member_id, std::string role);
};

bool IsSystemKey(const std::string& key);
bool IsConfigKey(const std::string& key);
bool IsHeartbeatKey(const std::string& key);

}

namespace backoff {
bool ExponentialBackoff(
    std::function<bool(void)> job,
    int interval_ms,
    int timeout_ms,
    float multiplier
);
}

#endif //CREDIS_CHAIN_H
