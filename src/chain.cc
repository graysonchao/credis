//
// Created by Grayson Chao on 3/1/18.
//

#include <string>
#include "src/nlohmann/json.hpp"
#include <thread>
#include "chain.h"
#include "glog/logging.h"

using namespace chain;
using json = nlohmann::json;

Chain::Chain(std::string app_prefix, std::string chain_id)
    : app_prefix(app_prefix), id(chain_id) { }

Member Chain::Head() {
  return members[head_id];
}

Member Chain::Tail() {
  return members[tail_id];
}

void Chain::AddMember(
    Member new_tail
) {
  auto member_it = members.find(new_tail.id);
  if (member_it != members.end()) {
    DLOG(INFO) << "Readding a member, " << id;
  }
  members[new_tail.id] = new_tail;
  if (members.size() == 1) {
    SetRole(new_tail.id, kRoleSingleton);
  } else {
    auto old_tail_id = tail_id;
    SetRole(new_tail.id, kRoleTail);
    if (members.size() == 2) {
      SetRole(old_tail_id, kRoleHead);
    } else {
      SetRole(old_tail_id, kRoleMiddle);
    }
    SetNext(old_tail_id, new_tail.id);
    SetPrev(new_tail.id, old_tail_id);
  }
}

bool Chain::HasMember(int64_t id) {
  return members.find(id) != members.end();
}

// Remove a member from the chain, repointing its neighbors appropriately.
void Chain::RemoveMember(int64_t member_id) {
  if (HasMember(member_id)) {
    Member failed = members[member_id];
    if (failed.config.role == chain::kRoleHead) {
      SetRole(failed.config.next, chain::kRoleHead);
      SetPrev(failed.config.next, chain::kNoMember);
    } else if (failed.config.role == kRoleTail) {
      SetRole(failed.config.prev, kRoleTail);
      SetNext(failed.config.prev, chain::kNoMember);
    } else if (failed.config.role == kRoleMiddle) {
      SetNext(failed.config.prev, failed.config.next);
      SetPrev(failed.config.next, failed.config.prev);
    }
    members.erase(member_id);

    if (members.empty()) {
      head_id = kNoMember;
      tail_id = kNoMember;
    } else if (members.size() == 1) {
      SetRole(members.begin()->first, kRoleSingleton);
      SetPrev(members.begin()->first, kNoMember);
      SetNext(members.begin()->first, kNoMember);
    }
  }
}

/**
 * Return all of the key-value pairs, as strings, that represent this chain in durable storage.
 * Iterates the chain first and removes any dead members.
 * @return
 */
std::vector<std::pair<std::string, std::string>> Chain::SerializedState() {
  std::vector<std::pair<std::string, std::string>> intended_state;
  if (members.empty()) {
    LOG(INFO) << "Serialized an empty chain because no members were found.";
    return intended_state;
  }

  if (members.size() == 1) {
    LOG(INFO) << "Serializing a singleton chain.";
    auto m = members.begin()->second;
    CHECK(m.HasHeartbeat() && m.HasConfig())
    << "Invalid member encountered during serialization: "
    << "heartbeat or config not found for ID " << m.id;
    CHECK(m.config.role == "singleton")
    << "Invalid member encountered during serialization: "
    << "the only member of a 1-node chain should have role=singleton.";
    intended_state.emplace_back(ConfigKey(m.id), m.config.ToJSON());
    return intended_state;
  }

  auto current = members.find(head_id);
  if (current == members.end()) {
    LOG(INFO) << "Serialized an empty chain because the head"
              << "(" << head_id << ") was not found.";
  }
  while (current != members.end()) {
    auto m = current->second;
    CHECK(m.HasHeartbeat() && m.HasConfig())
    << "Invalid member encountered during serialization: "
    << "heartbeat or config not found for ID " << m.id;
    if (m.config.role != kRoleTail) {
      CHECK(m.config.next != kNoMember)
      << "Head does not lead to the tail! "
      << m.id << " has role=" << m.config.role << ", but next=" << m.config.next;
    }
    current = members.find(m.config.next);
    intended_state.emplace_back(ConfigKey(m.id), m.config.ToJSON());
    if (m.config.role == kRoleTail) {
      return intended_state;
    }
  }
  return intended_state;
}

std::string Chain::ConfigKey(int64_t member_id) const {
  return MemberKey(
      app_prefix,
      id,
      member_id,
      kKeyTypeConfig
  ).ToString();
}

std::string Chain::MemberDirectory(int64_t member_id) const {
  return app_prefix + ":" + id + "/" + std::to_string(member_id);
}

void Chain::SetNext(int64_t member_id, int64_t next_id) {
  CHECK(HasMember(member_id))
  << "Tried to set child of a nonexistent member, ID=" << member_id;
  members[member_id].config.next = next_id;
  DLOG(INFO) << "Set member " << member_id << " child to " << next_id;
}

void Chain::SetPrev(int64_t member_id, int64_t prev_id) {
  CHECK(HasMember(member_id))
  << "Tried to set parent of a nonexistent member, ID=" << member_id;
  members[member_id].config.prev = prev_id;
  DLOG(INFO) << "Set member " << member_id << " parent to " << prev_id;
}

void Chain::SetRole(int64_t member_id, std::string role) {
  CHECK(HasMember(member_id))
  << "Tried to set role of a nonexistent member, ID=" << member_id;

  members[member_id].config.role = role;
  if (role == kRoleHead) {
    head_id = member_id;
  } else if (role == kRoleTail) {
    tail_id = member_id;
  } else if (role == kRoleSingleton) {
    head_id = member_id;
    tail_id = member_id;
  }
}

/**
 * Represents a key describing a member.
 * @param key_str
 */
MemberKey::MemberKey(const std::string &key_str) {
  // example string: credis:chain_id/member_id/config
  std::vector<std::string> components;
  std::string delimiters = ":/";
  size_t current;
  size_t next = -1;
  do
  {
    current = next + 1;
    next = key_str.find_first_of( delimiters, current );
    components.push_back(key_str.substr( current, next - current ));
  }
  while (next != std::string::npos);
  app_prefix = components[0];
  chain_id  = components[1];
  member_id = std::stol(components[2]);
  type = components[3];
}
MemberKey::MemberKey(
    std::string app_prefix,
    std::string chain_id,
    int64_t member_id,
    std::string type
) : app_prefix(app_prefix),
    chain_id(chain_id),
    member_id(member_id),
    type(type) {}

std::string MemberKey::ToString() const {
  return app_prefix + ":" + chain_id + "/" +
      std::to_string(member_id) + "/" + type;
}

/**
 * Represents intended config data for a member.
 */
MemberConfig::MemberConfig():
    role(chain::kRoleUninitialized),
    prev(chain::kNoMember),
    next(chain::kNoMember) {
}

MemberConfig::MemberConfig(const std::string &json_str) {
  json config_data = json::parse(json_str);
  role = config_data["role"];
  prev = config_data["prev"];
  next = config_data["next"];
}

MemberConfig::MemberConfig(
    std::string _role,
    int64_t _prev,
    int64_t _next
) {
  role = _role;
  prev = _prev;
  next = _next;
}

bool MemberConfig::operator==(const MemberConfig& other) {
  return role == other.role && prev == other.prev && /**/next == other.next;
}

bool MemberConfig::operator!=(const MemberConfig& other) {
  return !(*this == other);
}

bool MemberConfig::IsInitialized() {
  if (role == chain::kRoleSingleton) {
    return true;
  } else if (role == chain::kRoleHead) {
    return next != chain::kNoMember;
  } else if (role == chain::kRoleTail) {
    return prev != chain::kNoMember;
  } else {
    return next != chain::kNoMember && prev != chain::kNoMember;
  }
}

std::string MemberConfig::ToJSON() const {
  return json{
      {"role", role},
      {"prev", prev},
      {"next", next},
  }.dump();
}

/**
 * Represents heartbeat (conn info + current state) data for a member.
 * @param json_str
 */
MemberHeartbeat::MemberHeartbeat() = default;

MemberHeartbeat::MemberHeartbeat(const std::string &json_str) {
  LOG(INFO) << json_str;
  json hb_data = json::parse(json_str);
  config = MemberConfig(hb_data["role"], hb_data["prev"], hb_data["next"]);
  address = hb_data["address"];
  port =  hb_data["port"];
};

std::string MemberHeartbeat::ToJSON() const {
  return json{
      {"address", address},
      {"port", port},
      {"role", config.role},
      {"prev", config.prev},
      {"next", config.next}
  }.dump();
}

bool MemberHeartbeat::IsInitialized() {
  return address != "";
}

MemberHeartbeat::MemberHeartbeat(std::string address,
                                 int port,
                                 std::string role,
                                 int64_t prev,
                                 int64_t next)
    : address(address), port(port), config(MemberConfig(role, prev, next)) {}

Member::Member(int64_t id): id(id) {}
Member::Member(): id(kNoMember) {}
bool Member::HasHeartbeat() {
  return heartbeat.IsInitialized();
}
bool Member::HasConfig() {
  return config.IsInitialized();
}

/**
 * Repeatedly try to call JOB with exponential backoff, returning true if successful.
 * The actual timeout is not guaranteed to be exactly the timeout given.
 * @param interval_ms
 * @param timeout_ms
 * @param multiplier
 * @param job
 */
bool backoff::ExponentialBackoff(
    std::function<bool(void)> job,
    int interval_ms,
    int timeout_ms,
    float multiplier
) {
  int elapsed_ms = 0;
  while (!job()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    elapsed_ms += interval_ms;
    interval_ms *= multiplier;
    if (elapsed_ms > timeout_ms) {
      return false;
    }
  }
  return true;
}

