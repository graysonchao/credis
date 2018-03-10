//
// Created by Grayson Chao on 3/1/18.
//

#include <boost/optional/optional.hpp>
#include <string>
#include <map>
#include <map>

#ifndef CREDIS_CHAIN_H
#define CREDIS_CHAIN_H

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
      std::string app_prefix,
      std::string chain_id,
      int64_t member_id,
      std::string type
  );
  std::string ToString() const;

  std::string app_prefix;
  std::string chain_id;
  int64_t member_id;
  std::string type;
};
class MemberConfig {
 public:
  MemberConfig();
  explicit MemberConfig(const std::string &json_str);
  explicit MemberConfig(
      std::string role,
      int64_t prev,
      int64_t next
  );
  bool operator==(const MemberConfig& other);
  bool operator!=(const MemberConfig& other);
  std::string ToJSON() const;
  bool IsInitialized();
  std::string role;
  int64_t prev;
  int64_t next;
};
class MemberHeartbeat {
 public:
  MemberHeartbeat();
  explicit MemberHeartbeat(const std::string &json_str);
  explicit MemberHeartbeat(
      std::string address,
      int port,
      std::string role,
      int64_t prev,
      int64_t next
  );
  std::string ToJSON() const;

  bool IsInitialized();
  MemberConfig config;
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
  MemberConfig config;
  MemberHeartbeat heartbeat;
};

/**
 * The Chain class represents a CR chain.
 * It provides a semantic interface (Head(), Tail(), etc.) and the ability
 * to serialize a full set of intended configs.
 */
class Chain {
 public:
  Chain(std::string app_prefix, std::string chain_id);
  void AddMember(Member new_tail);
  void RemoveMember(int64_t id);
  bool HasMember(int64_t id);

  Member Head();
  Member Tail();

  std::vector<std::pair<std::string, std::string>> SerializedState();
  std::string ConfigKey(int64_t member_id) const;
  std::string MemberDirectory(int64_t member_id) const;

  std::map<int64_t, Member> members;
  std::string app_prefix;
  std::string id;
  int64_t head_id;
  int64_t tail_id;

 private:
  void SetNext(int64_t member_id, int64_t next_id);
  void SetPrev(int64_t member_id, int64_t prev_id);
  void SetRole(int64_t member_id, std::string role);
};

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
