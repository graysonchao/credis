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
        std::string ToJSON() const;
        std::string role;
        int64_t prev;
        int64_t next;
    };
    class MemberHeartbeat {
    public:
        MemberHeartbeat();
        explicit MemberHeartbeat(const std::string &json_str);
        std::string ToJSON() const;
        std::string address;
        int port;
        MemberConfig config;
    };
    class Member {
    public:
        Member();
        explicit Member(int64_t id);
        Member(int64_t id, MemberConfig config, MemberHeartbeat heartbeat);
        int64_t id;
        boost::optional<MemberConfig> config;
        boost::optional<MemberHeartbeat> heartbeat;
    };

    /**
     * The Chain class represents a CR chain.
     * It provides a semantic interface (Head(), Tail(), etc.) and the ability
     * to serialize a full set of intended configs.
     */
    class Chain {
    public:
        Chain(
            std::string app_prefix,
            std::string chain_id,
            std::map<std::string, std::string> initial_state
        );
        void AddMember(int64_t id, std::string hb_json);
        std::vector<std::pair<std::string, std::string>> SerializedState();
        void SetNext(int64_t member_id, int64_t next_id);
        void SetPrev(int64_t member_id, int64_t prev_id);
        void SetRole(int64_t member_id, std::string role);
        std::string ConfigPath(int64_t member_id) const;
        std::string HeartbeatPath(int64_t member_id) const;
        std::vector<Member> EnforceChain();
        std::string ToString();

        std::map<int64_t, Member> members;
        std::string app_prefix;
        std::string id;
        int64_t head_id;
        int64_t tail_id;
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
