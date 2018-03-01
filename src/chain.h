//
// Created by Grayson Chao on 3/1/18.
//

#include <boost/optional/optional.hpp>
#include <string>
#include <map>
#include <unordered_map>

#ifndef CREDIS_CHAIN_H
#define CREDIS_CHAIN_H

namespace chain {
    const std::string kRoleHead = "head";
    const std::string kRoleTail = "tail";
    const std::string kRoleMiddle = "middle";
    const std::string kRoleSingleton = "singleton";
    const std::string kRoleUninitialized = "uninitialized";
    const std::string kNoMember = "nil";

    class MemberKey {
    public:
        explicit MemberKey(const std::string &_key_str);
        MemberKey(
            const std::string &app_prefix,
            const std::string &chain_id,
            const std::string &member_id,
            const std::string &type
        );
        std::string ToString() const;

        std::string app_prefix;
        std::string chain_id;
        std::string member_id;
        std::string type;
    };
    class MemberConfig {
    public:
        MemberConfig();
        explicit MemberConfig(const std::string &json_str);
        explicit MemberConfig(
            const std::string &role,
            const std::string &prev,
            const std::string &next
        );
        std::string ToJSON() const;
        std::string role;
        std::string prev;
        std::string next;
    };
    class MemberHeartbeat {
    public:
        MemberHeartbeat();
        explicit MemberHeartbeat(const std::string &json_str);
        std::string ToJSON() const;
        std::string address;
        std::string port;
        MemberConfig config;
    };
    class Member {
    public:
        Member();
        explicit Member(std::string id);
        Member(std::string id, MemberConfig config, MemberHeartbeat heartbeat);
        std::string id;
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
            const std::string &app_prefix,
            const std::string &chain_id,
            std::unordered_map<std::string, std::string> initial_state
        );
        void AddMember(const std::string &id, const std::string &hb_json);
        std::vector<std::pair<std::string, std::string>> SerializedState() const;
        void SetNext(const std::string &member_id, const std::string &next_id );
        void SetPrev(const std::string &member_id, const std::string &prev_id );
        void SetRole(const std::string &member_id, const std::string &role);
        std::string ConfigPath(const std::string &member_id) const;

        std::unordered_map<std::string, Member> members;
        std::string app_prefix;
        std::string id;
        std::string head_id;
        std::string tail_id;
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
