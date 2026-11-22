// Daily columns borrow names from input mappings and shard-owned storage until save().
#pragma once
#include "native_input.hpp"
#include "parquet_min.hpp"

namespace daily_output {
struct UserColumns {
    std::vector<Text> names, categories;
    std::vector<uint8_t> present;
    std::vector<int32_t> days;
    std::vector<int64_t> sessions, events, revenues;

    void append(Text name, int32_t day, int64_t ns, int64_t ne, int64_t revenue, Text top) {
        names.push_back(name);
        days.push_back(day);
        sessions.push_back(ns);
        events.push_back(ne);
        revenues.push_back(revenue);
        present.push_back(revenue > 0);
        if (revenue > 0) categories.push_back(top);
    }

    void write_part(const fs::path& path) const {
        pq::Table table;
        table.add_str("user_id", names);
        table.add_date("day", days);
        table.add_i64("sessions", sessions);
        table.add_i64("events", events);
        table.add_i64("revenue_cents", revenues);
        table.add_str("top_category", categories, &present);
        table.write_file(path.string());
    }
};

struct CampaignColumns {
    std::vector<Text> ids, channels;
    std::vector<int32_t> days;
    std::vector<int64_t> sessions, purchases, revenues, assists;

    void append(Text id, Text channel, int32_t day, int64_t ns, int64_t np, int64_t revenue, int64_t na) {
        ids.push_back(id);
        channels.push_back(channel);
        days.push_back(day);
        sessions.push_back(ns);
        purchases.push_back(np);
        revenues.push_back(revenue);
        assists.push_back(na);
    }

    void write_part(const fs::path& path) const {
        pq::Table table;
        table.add_str("campaign_id", ids);
        table.add_str("channel", channels);
        table.add_date("day", days);
        table.add_i64("sessions", sessions);
        table.add_i64("purchasing_sessions", purchases);
        table.add_i64("revenue_cents", revenues);
        table.add_i64("assisted_sessions", assists);
        table.write_file(path.string());
    }
};
}
