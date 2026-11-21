#include "native_input.hpp"
#include <map>
#include <tuple>

namespace {
constexpr size_t shards=64;
size_t shard(Text key) { return std::hash<Text>{}(key) % shards; }

struct Dimensions {
    std::unordered_map<Text,int> products, campaigns;
    std::vector<Text> categories{"unknown"}, campaign_names{"organic"}, channels{"organic"};
    Dimensions(Inputs& input, const fs::path& data) {
        std::unordered_map<Text,int> categories_by_name{{"unknown",0}};
        for (Text block : input.chunks(data/"products.csv")) scan<2>(block,[&](const auto& f){
            auto [it,fresh]=categories_by_name.emplace(f[1],categories.size());
            if (fresh) categories.push_back(f[1]);
            products.emplace(f[0],it->second);
        });
        for (Text block : input.chunks(data/"campaigns.csv")) scan<2>(block,[&](const auto& f){
            campaigns.emplace(f[0],campaign_names.size());
            campaign_names.push_back(f[0]); channels.push_back(f[1]);
        });
    }
};

struct Sale { Millis t; int cents, category; };
struct Candidate { Text id, user; Millis ingest; Sale sale; };
struct Click { Text user, id; Millis t; int campaign; };
struct Event { Text person; Sale sale; };
template<class Row> using Buckets=std::array<std::vector<Row>,shards>;

void read_events(Text block, Buckets<Candidate>& dst, const Dimensions& dim, Millis lo, Millis hi) {
    scan<7>(block,[&](const auto& f){
        if (f[0].empty()) return;
        Millis t=timestamp(f[2]), ingest=timestamp(f[3]);
        Sale sale{invalid_time,0,0};
        if (t>=lo && t<hi && ingest!=invalid_time && ingest-t<=day_ms && !f[1].empty() && !f[5].empty()) {
            if (f[4]=="purchase") {
                Millis price=decimal(f[6]);
                if (price>=1 && price<=10000000) {
                    auto it=dim.products.find(f[5]);
                    sale={t,int(price),it==dim.products.end() ? 0 : it->second};
                }
            } else if (f[4]=="view" || f[4]=="cart") sale.t=t;
        }
        // A well-formed invalid row still competes for earliest ingest.
        dst[shard(f[0])].push_back({f[0],f[1],ingest,sale});
    });
}

void dedup(size_t bucket, std::vector<Buckets<Candidate>>& blocks,
           Buckets<Event>& survivors, const Identities& identities) {
    size_t count=0;
    for (auto& block : blocks) count+=block[bucket].size();
    size_t capacity=1;
    while (capacity < count+count/2+1) capacity*=2;
    std::vector<const Candidate*> winners(capacity);
    for (auto& block : blocks) for (const auto& row : block[bucket]) {
        size_t slot=(std::hash<Text>{}(row.id)/shards) & (capacity-1);
        while (winners[slot] && winners[slot]->id!=row.id) slot=(slot+1)&(capacity-1);
        if (!winners[slot] || row.ingest<winners[slot]->ingest) winners[slot]=&row;
    }
    for (const Candidate* row : winners) if (row && row->sale.t!=invalid_time) {
        Text person=identities.person(row->user);
        survivors[shard(person)].push_back({person,row->sale});
    }
    for (auto& block : blocks) std::vector<Candidate>().swap(block[bucket]);
}

// Restricted CSV values may contain CR but never a quote, comma or LF.
class Writer {
    std::ofstream file;
    std::string buffer;
public:
    explicit Writer(const fs::path& path) : file(path,std::ios::binary) {
        file.exceptions(std::ios::badbit|std::ios::failbit);
        buffer.reserve(256*1024);
    }
    void text(Text s) { buffer.push_back('"'); buffer.append(s); buffer.push_back('"'); }
    void number(int64_t value) {
        char tmp[32];
        auto r=std::to_chars(tmp,tmp+sizeof(tmp),value);
        buffer.append(tmp,r.ptr);
    }
    void comma() {buffer.push_back(',');}
    void line() {
        buffer.push_back('\n');
        if (buffer.size()>=128*1024) {file.write(buffer.data(),buffer.size()); buffer.clear();}
    }
    void close() {file.write(buffer.data(),buffer.size()); file.close();}
};

struct IndexedEvent { unsigned person; Sale sale; };
struct IndexedClick { unsigned person; int campaign; Millis t; Text id; };
struct Daily { int64_t sessions=0, events=0, revenue=0; };
struct Credit { int64_t sessions=0, purchases=0, revenue=0, assists=0; };

void process(size_t bucket, std::vector<Buckets<Event>>& events_by_id,
             std::vector<Buckets<Click>>& clicks_by_file, const Dimensions& dim, const fs::path& scratch) {
    std::unordered_map<Text,unsigned> persons;
    std::vector<Text> names;
    auto person_id=[&](Text person){
        auto [it,fresh]=persons.emplace(person,names.size());
        if (fresh) names.push_back(person);
        return it->second;
    };
    std::vector<IndexedEvent> events;
    for (auto& set : events_by_id) {
        for (const auto& e : set[bucket]) events.push_back({person_id(e.person),e.sale});
        std::vector<Event>().swap(set[bucket]);
    }
    std::vector<IndexedClick> clicks;
    for (auto& set : clicks_by_file) {
        for (const auto& c : set[bucket]) {
            auto it=persons.find(c.user);
            if (it!=persons.end()) clicks.push_back({it->second,c.campaign,c.t,c.id});
        }
        std::vector<Click>().swap(set[bucket]);
    }
    std::sort(events.begin(),events.end(),[](const auto& a,const auto& b){
        return std::tie(a.person,a.sale.t)<std::tie(b.person,b.sale.t);
    });
    std::sort(clicks.begin(),clicks.end(),[](const auto& a,const auto& b){
        if (a.person!=b.person) return a.person<b.person;
        if (a.t!=b.t) return a.t<b.t;
        return a.id>b.id; // The last eligible click wins the bytewise tie.
    });

    Writer users(scratch/("user-"+std::to_string(bucket)+".csv"));
    std::map<std::pair<Millis,int>,Credit> campaigns;
    std::unordered_map<int,int64_t> category_totals;
    std::unordered_map<int,unsigned> active;
    size_t e=0, next=0;
    while (e<events.size()) {
        unsigned person=events[e].person;
        while (next<clicks.size() && clicks[next].person<person) ++next;
        size_t oldest=next;
        active.clear();
        Millis current_day=invalid_time;
        Daily daily;
        auto finish_day=[&]{
            if (current_day==invalid_time) return;
            Text top;
            int64_t best=0;
            for (auto [cat,amount] : category_totals) {
                Text name=dim.categories[cat];
                if (amount>best || (amount==best && name<top)) {best=amount; top=name;}
            }
            users.text(names[person]); users.comma(); users.number(current_day); users.comma();
            users.number(daily.sessions); users.comma(); users.number(daily.events); users.comma();
            users.number(daily.revenue); users.comma(); if (best) users.text(top);
            users.line(); daily={}; category_totals.clear();
        };
        while (e<events.size() && events[e].person==person) {
            Millis start=events[e].sale.t;
            Millis day=start/day_ms-(start%day_ms<0);
            if (day!=current_day) {finish_day(); current_day=day;}
            while (next<clicks.size() && clicks[next].person==person && clicks[next].t<=start) {
                ++active[clicks[next].campaign]; ++next;
            }
            while (oldest<next && clicks[oldest].t<start-week_ms) {
                auto it=active.find(clicks[oldest++].campaign);
                if (--it->second==0) active.erase(it);
            }
            int winner=oldest==next ? 0 : clicks[next-1].campaign;
            for (const auto& pair : active) ++campaigns[{day,pair.first}].assists;
            Millis prev;
            int64_t revenue=0;
            ++daily.sessions;
            do {
                const Sale& sale=events[e++].sale;
                ++daily.events; revenue+=sale.cents;
                if (sale.cents) category_totals[sale.category]+=sale.cents;
                prev=sale.t;
            } while (e<events.size() && events[e].person==person && events[e].sale.t-prev<=1800000);
            daily.revenue+=revenue;
            auto& credit=campaigns[{day,winner}];
            ++credit.sessions; credit.purchases+=revenue>0; credit.revenue+=revenue;
        }
        finish_day();
    }
    users.close();
    Writer ads(scratch/("campaign-"+std::to_string(bucket)+".csv"));
    for (const auto& [key,counts] : campaigns) {
        auto [day, index]=key;
        ads.text(dim.campaign_names[index]); ads.comma(); ads.text(dim.channels[index]); ads.comma();
        ads.number(day); ads.comma(); ads.number(counts.sessions); ads.comma(); ads.number(counts.purchases);
        ads.comma(); ads.number(counts.revenue); ads.comma(); ads.number(counts.assists); ads.line();
    }
    ads.close();
}
}

int main(int argc,char** argv) {
    if (argc!=6) return 2;
    try {
        fs::path data=argv[1], scratch=argv[2];
        Millis lo=timestamp(argv[3]), hi=timestamp(argv[4]);
        if (lo==invalid_time || hi==invalid_time || hi<=lo) throw std::runtime_error("invalid window");
        unsigned workers=std::clamp(std::stoi(argv[5]),1,64);
        using Clock=std::chrono::steady_clock;
        auto previous=Clock::now();
        auto log=[&](const char* stage) {
            auto now=Clock::now();
            std::cerr<<stage<<" "<<std::chrono::duration<double>(now-previous).count()<<"s\n"; previous=now;
        };
        Inputs input;
        Dimensions dim(input,data);
        Identities ids(input.chunks(data/"links"));
        auto ev=input.chunks(data/"events"), cl=input.chunks(data/"clicks");
        log("load identities and map files");
        std::vector<Buckets<Candidate>> raw(ev.size());
        parallel(ev.size(),workers,[&](size_t i){read_events(ev[i],raw[i],dim,lo,hi);});
        log("parse events");
        std::vector<Buckets<Event>> valid(shards);
        parallel(shards,workers,[&](size_t i){dedup(i,raw,valid[i],ids);});
        log("deduplicate and resolve people");
        std::vector<Buckets<Click>> clicks(cl.size());
        parallel(cl.size(),workers,[&](size_t i){
            scan<4>(cl[i],[&](const auto& f){
                if (f[0].empty() || f[1].empty()) return;
                auto campaign=dim.campaigns.find(f[2]);
                Millis t=timestamp(f[3]);
                if (campaign==dim.campaigns.end() || t<lo-week_ms || t>=hi) return;
                Text person=ids.person(f[1]);
                clicks[i][shard(person)].push_back({person,f[0],t,campaign->second});
            });
        });
        log("parse clicks");
        parallel(shards,workers,[&](size_t i){process(i,valid,clicks,dim,scratch);});
        log("session and attribution sweep");
    } catch (const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
