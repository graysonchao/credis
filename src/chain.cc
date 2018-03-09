//
// Created by Grayson Chao on 3/1/18.
//

#include <boost/optional/optional.hpp>
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include <string>
#include <thread>
#include "chain.h"
#include "nlohmann/json.hpp"

using namespace chain;
using json = nlohmann::json;

Chain::Chain(
    std::string app_prefix,
    std::string chain_id,
    std::map<std::string, std::string> initial_state
) : app_prefix(app_prefix), id(chain_id) {
  for (auto it : initial_state) {
    MemberKey key(it.first);
    members[key.member_id].id = key.member_id;
    if (key.type == "hb") {
      LOG(INFO) << "Chain read heartbeat for " << key.member_id << ": " << it.second;
      members[key.member_id].heartbeat.emplace(it.second);
    } else if (key.type == "config") {
      LOG(INFO) << "Chain read config for " << key.member_id << ": " << it.second;
      members[key.member_id].config.emplace(it.second);
    }
  }
  for (auto &it: members) {
    if (!it.second.config.is_initialized()) {
      it.second.config.emplace();
    }
  }
}

/**
 * Given JSON for a heartbeat, adds a member with no config.
 * @param id
 * @param hb_json
 * @return
 */
void Chain::AddMember(
    int64_t id,
    std::string hb_json
) {
  auto member_it = members.find(id);
  if (member_it != members.end()) {
    LOG(INFO) << "Readding a member, " << id;
  }
  members[id].id = id;
  members[id].heartbeat.emplace(hb_json);
  if (members.size() == 1) {
    members[id].config.emplace(kRoleSingleton, kNoMember, kNoMember);
    head_id = id;
    tail_id = id;
  } else {
    auto old_tail_id = tail_id;
    members[id].config.emplace(kRoleUninitialized, old_tail_id, kNoMember);
    SetRole(id, kRoleTail);
    if (members.size() == 2) {
      SetRole(old_tail_id, kRoleHead);
    } else {
      SetRole(old_tail_id, kRoleMiddle);
    }
    SetNext(old_tail_id, id);
    SetPrev(id, old_tail_id);
  }
}

/**
 * Return all of the key-value pairs, as strings, that represent this chain in durable storage.
 * It is an error to call this method when any of the chain's members are missing a heartbeat or config.
 * It is also an error to call this method if the chain is not a properly formed linked list.
 * @return
 */
std::vector<std::pair<std::string, std::string>> Chain::SerializedState() {
  std::vector<std::pair<std::string, std::string>> intended_state;

  EnforceChain();

  if (members.empty()) {
    LOG(INFO) << "Serialized an empty chain because no members were found.";
    return intended_state;
  }

  if (members.size() == 1) {
    LOG(INFO) << "Serializing a singleton chain.";
    auto m = members.begin()->second;
    CHECK(m.heartbeat.is_initialized() && m.config.is_initialized())
        << "Invalid member encountered during serialization: "
        << "heartbeat or config not found for ID " << m.id;
    CHECK(m.config->role == "singleton")
        << "Invalid member encountered during serialization: "
        << "the only member of a 1-node chain should have role=singleton.";
    intended_state.emplace_back(ConfigPath(m.id), m.config->ToJSON());
    return intended_state;
  }

  auto current = members.find(head_id);
  if (current == members.end()) {
    LOG(INFO) << "Serialized an empty chain because the head"
              << "(" << head_id << ") was not found.";
  }
  while (current != members.end()) {
    auto m = current->second;
    CHECK(m.heartbeat.is_initialized() && m.config.is_initialized())
        << "Invalid member encountered during serialization: "
        << "heartbeat or config not found for ID " << m.id;
    if (m.config->role != kRoleTail) {
      CHECK(m.config->next != kNoMember)
          << "Head does not lead to the tail! "
          << m.id << " has role=" << m.config->role << ", but next=" << m.config->next;
    }
    current = members.find(m.config->next);
    intended_state.emplace_back(ConfigPath(m.id), m.config->ToJSON());
    if (m.config->role == kRoleTail) {
      return intended_state;
    }
  }
  return intended_state;
}

/**
 * Enforce that the state of the members represents a working chain.
 * Mutates the Chain by removing any members that don't have heartbeats.
 * Returns the members that were removed.
 * @param foreach
 */
std::vector<Member> Chain::EnforceChain() {
  std::vector<Member> deleted;
  auto current = members.begin();
  while (current != members.end()) {
    if (!current->second.heartbeat.is_initialized()) {
      deleted.push_back(current->second);
      auto to_delete = current->first;
      current++;
      members.erase(to_delete);
    } else {
      current++;
    }
  }
  if (members.empty()) {
    return deleted;
  }

  current = members.begin();
  if (members.size() == 1) {
    head_id = current->first;
    tail_id = current->first;
    SetRole(current->first, kRoleSingleton);
    return deleted;
  }
  SetRole(current->first, kRoleHead);
  SetPrev(current->first, kNoMember);

  head_id = current->first;
  auto last = current;
  current++;
  while (current != members.end()) {
    SetNext(last->first, current->first);
    SetPrev(current->first, last->first);
    SetRole(current->first, kRoleMiddle);
    last = current;
    current++;
  }
  tail_id = last->first;
  SetRole(last->first, kRoleTail);
  SetNext(last->first, kNoMember);

  LOG(INFO) << "Chain after enforcing: ";
  for (auto it: members) {
    LOG(INFO) << it.first << ": "
              << it.second.config->ToJSON();
  }
  return deleted;
}

std::string Chain::ToString() {
  std::string output;
  for (auto it: members) {
    output = output + std::to_string(it.first) + ", ";
  }
  return output;
}

std::string Chain::ConfigPath(const int64_t member_id) const {
  return MemberKey(
      app_prefix,
      id,
      member_id,
      "config"
  ).ToString();
}

std::string Chain::HeartbeatPath(const int64_t member_id) const {
  return MemberKey(
      app_prefix,
      id,
      member_id,
      "hb"
  ).ToString();
}

void Chain::SetNext(int64_t member_id, int64_t next_id) {
  auto it = members.find(member_id);
  if (it != members.end()) {
    auto &m = it->second;
    if (m.config.is_initialized()) {
      m.config.emplace(m.config->role, m.config->prev, next_id);
    } else {
      m.config.emplace(kRoleUninitialized, kNoMember, next_id);
    }
    LOG(INFO) << "Set member " << member_id << " next_id to " << next_id;
  } else {
    LOG(INFO) << "Tried to set next_id for a nonexistent member ID, " << member_id;
  }
}

void Chain::SetPrev(int64_t member_id, int64_t prev_id) {
  auto it = members.find(member_id);
  if (it != members.end()) {
    auto &m = it->second;
    if (m.config.is_initialized()) {
      m.config.emplace(m.config->role, prev_id, m.config->next);
    } else {
      m.config.emplace(kRoleUninitialized, prev_id, kNoMember);
    }
    LOG(INFO) << "Set member " << member_id << " prev to " << prev_id;
  } else {
    LOG(INFO) << "Tried to set prev for a nonexistent member ID, " << member_id;
  }
}

void Chain::SetRole(int64_t member_id, std::string role) {
  auto it = members.find(member_id);
  if (it != members.end()) {
    auto &m = it->second;
    if (m.config.is_initialized()) {
      m.config.emplace(role, m.config->prev, m.config->next);
    } else {
      m.config.emplace(role, kNoMember, kNoMember);
    }
    if (role == kRoleHead) {
      head_id = member_id;
    } else if (role == kRoleTail) {
      tail_id = member_id;
    } else if (role == kRoleSingleton) {
      head_id = member_id;
      tail_id = member_id;
    }
    LOG(INFO) << "Set member " << member_id << " role to " << role;
  } else {
    LOG(INFO) << "Tried to set role for a nonexistent member ID, " << member_id;
  }
}
bool Chain::HasMember(int64_t id) {
  return members.find(id) != members.end();
}

/**
 * Represents a key describing a member.
 * @param key_str
 */
MemberKey::MemberKey(const std::string &key_str) {
  // example string: credis:chain_id/member_id/config
  std::vector<std::string> components;
  boost::split(components, key_str, [](char c){return c == '/' || c == ':';});
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
    next(chain::kNoMember) {}
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

Member::Member(int64_t id): id(id) {}
Member::Member(int64_t id, MemberConfig config, MemberHeartbeat heartbeat)
    : id(id), config(config), heartbeat(heartbeat) {}

Member::Member(): id(kNoMember) { }


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

