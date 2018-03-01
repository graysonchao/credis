//
// Created by Grayson Chao on 3/1/18.
//

#include <boost/optional/optional.hpp>
#include <boost/algorithm/string.hpp>
#include <string>
#include <src/nlohmann/json.hpp>
#include <thread>
#include "chain.h"
#include "glog/logging.h"

using namespace chain;
using json = nlohmann::json;

Chain::Chain(
    const std::string &app_prefix,
    const std::string &chain_id,
    std::unordered_map<std::string, std::string> initial_state
) : app_prefix(app_prefix), id(chain_id) {
  for (auto &it : initial_state) {
    MemberKey key(it.first);
    auto &m = members[key.member_id];
    m.id = key.member_id;
    if (key.type == "hb") {
      m.heartbeat.emplace(it.second);
    } else if (key.type == "config") {
      m.config.emplace(it.second);
      if (m.config->role == kRoleHead) {
        head_id = m.id;
      } else if (m.config->role == kRoleTail) {
        tail_id = m.id;
      } else if (m.config->role == kRoleSingleton) {
        head_id = kNoMember;
        tail_id = kNoMember;
      }
    }
  }
  for (auto &it : initial_state) {
    MemberKey key(it.first);
    if (members[key.member_id].heartbeat == boost::none) {
      members.erase(key.member_id);
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
    const std::string &id,
    const std::string &hb_json
) {
  auto member_it = members.find(id);
  if (member_it == members.end()) {
    members.emplace(id, Member(id));
  }
  auto &m = members[id];
  m.id = id;
  m.heartbeat.emplace(hb_json);
  if (members.empty()) {
    m.config.emplace(kRoleSingleton, kNoMember, kNoMember);
  } else {
    m.config.emplace();
  }
}

/**
 * Return all of the key-value pairs, as strings, that represent this chain in durable storage.
 * It is an error to call this method when any of the chain's members are missing a heartbeat or config.
 * It is also an error to call this method if the chain is not a properly formed linked list.
 * @return
 */
std::vector<std::pair<std::string, std::string>> Chain::SerializedState() const {
  std::vector<std::pair<std::string, std::string>> intended_state;

  if (members.empty()) {
    LOG(INFO) << "Serialized an empty chain because no members were found.";
    return intended_state;
  }

  if (members.size() == 1) {
    LOG(INFO) << "Serializing a singleton chain.";
    auto m = members.begin()->second;
    CHECK(m.heartbeat != boost::none && m.config != boost::none)
        << "Invalid member encountered during serialization: "
        << "heartbeat or config not found for ID " << m.id;
    CHECK(m.config->next == kNoMember && m.config->prev == kNoMember)
        << "Invalid member encountered during serialization: "
        << "singleton should have next=nil, prev=nil.";
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
    CHECK(m.heartbeat != boost::none && m.config != boost::none)
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

std::string Chain::ConfigPath(const std::string &member_id) const {
  return MemberKey(
      app_prefix,
      id,
      member_id,
      "config"
  ).ToString();
}

void Chain::SetNext(const std::string &member_id, const std::string &next_id) {
  auto m = members.find(member_id);
  if (m != members.end()) {
    m->second.config->next = next_id;
    LOG(INFO) << "Set member " << member_id << " next_id to " << next_id;
  } else {
    LOG(INFO) << "Tried to set next_id for a nonexistent member ID, " << member_id;
  }
}

void Chain::SetPrev(const std::string &member_id, const std::string &prev_id) {
  auto m = members.find(member_id);
  if (m != members.end()) {
    m->second.config->prev = prev_id;
    LOG(INFO) << "Set member " << member_id << " prev to " << prev_id;
  } else {
    LOG(INFO) << "Tried to set prev for a nonexistent member ID, " << member_id;
  }
}

void Chain::SetRole(const std::string &member_id, const std::string &role) {
  auto m = members.find(member_id);
  if (m != members.end()) {
    m->second.config->role = role;
    if (role == kRoleHead) {
      head_id = member_id;
    } else if (role == kRoleTail) {
      tail_id = member_id;
    }
    LOG(INFO) << "Set member " << member_id << " role to " << role;
  } else {
    LOG(INFO) << "Tried to set role for a nonexistent member ID, " << member_id;
  }
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
  member_id = components[2];
  type = components[3];
}
MemberKey::MemberKey(
    const std::string &app_prefix,
    const std::string &chain_id,
    const std::string &member_id,
    const std::string &type
) : app_prefix(app_prefix),
    chain_id(chain_id),
    member_id(member_id),
    type(type) {}

std::string MemberKey::ToString() const {
  return app_prefix + ":" + chain_id + "/" + member_id + "/" + type;
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
    const std::string &_role,
    const std::string &_prev,
    const std::string &_next
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

Member::Member(std::string id): id(std::move(id)) {}
Member::Member(std::string id, MemberConfig config, MemberHeartbeat heartbeat)
    : id(std::move(id)), config(config), heartbeat(heartbeat) {}

Member::Member(): id(kNoMember) { }


// TODO: I'm homeless!
/**
 * Try repeatedly to call JOB with exponential backoff, returning true if successful.
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

