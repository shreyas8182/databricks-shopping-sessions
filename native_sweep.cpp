#include "parquet_output.hpp"
#include <map>
#include <tuple>

namespace {
constexpr size_t shards=64;
size_t shard(Key key) { return key_hash(key) % shards; }

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
struct Ticket { Key id; Millis ingest; uint32_t row, block; };
struct Click { Key user; Text id; Millis t; int campaign; };
struct Event { Key person; Sale sale; };
template<class Row> using Buckets=std::array<std::vector<Row>,shards>;
struct EventBlock { Buckets<Event> events; Buckets<Ticket> tickets; };

void read_events(Text input, uint32_t block, EventBlock& dst, const Dimensions& dim,
                 Keys& users, Keys& ids, const Identities& links, Millis lo, Millis hi) {
    scan<7>(input,[&](const auto& f){
        if (f[0].empty()) return;
        Key id=ids.encode(f[0]);
        Millis t=timestamp(f[2]), ingest=timestamp(f[3]);
        uint32_t row=UINT32_MAX;
        if (t>=lo && t<hi && ingest!=invalid_time && ingest-t<=day_ms && !f[1].empty() && !f[5].empty()) {
            Sale sale{t,0,0};
            if (f[4]=="purchase") {
                Millis price=decimal(f[6]);
                if (price>=1 && price<=10000000) {
                    auto it=dim.products.find(f[5]);
                    sale.cents=int(price); sale.category=it==dim.products.end()?0:it->second;
                } else sale.t=invalid_time;
            } else if (f[4]!="view" && f[4]!="cart") sale.t=invalid_time;
            if (sale.t!=invalid_time) {
                Key person=links.person(users.encode(f[1]));
                size_t lane=shard(person);
                auto& target=dst.events[lane];
                row=uint32_t(target.size()*shards+lane);
                target.push_back({person,sale});
            }
        }
        dst.tickets[shard(id)].push_back({id,ingest,row,block});
    });
}

void dedup(size_t lane, std::vector<EventBlock>& blocks) {
    size_t count=0;
    for (auto& block:blocks) count+=block.tickets[lane].size();
    size_t capacity=1; while(capacity<count+count/2+1) capacity*=2;
    struct Winner {Key key=0; const Ticket* row=nullptr;};
    std::vector<Winner> winners(capacity);
    for (auto& block:blocks) for (const auto& ticket:block.tickets[lane]) {
        size_t slot=(key_hash(ticket.id)/shards)&(capacity-1);
        while (winners[slot].key && winners[slot].key!=ticket.id) slot=(slot+1)&(capacity-1);
        auto& winner=winners[slot];
        if (!winner.key) {winner={ticket.id,&ticket}; continue;}
        const Ticket* loser=&ticket;
        if (ticket.ingest<winner.row->ingest) std::swap(loser,winner.row);
        // Each event belongs to exactly one ID lane. Distinct lanes can safely
        // mark their own losers without an atomic operation on every winner.
        if (loser->row!=UINT32_MAX)
            blocks[loser->block].events[loser->row%shards][loser->row/shards].sale.t=invalid_time;
    }
    for (auto& block:blocks) std::vector<Ticket>().swap(block.tickets[lane]);
}

struct IndexedEvent { unsigned person; Sale sale; };
struct IndexedClick { unsigned person; int campaign; Millis t; Text id; };
struct Daily { int64_t sessions=0, events=0, revenue=0; };
struct Credit { int64_t sessions=0, purchases=0, revenue=0, assists=0; };

using CampaignTotals=std::unordered_map<Millis,std::unordered_map<int,Credit>>;

// Most shoppers have a small number of rows. Count into contiguous
// ranges, then sort only within the individual shopper's range.
template<class Row,class Compare>
void order_by_person(std::vector<Row>& rows,size_t people,Compare compare) {
    std::vector<size_t> ends(people);
    for (const Row& row:rows) ++ends[row.person];
    size_t total=0;
    for (auto& n:ends) {size_t count=n; n=total; total+=count;}
    std::vector<Row> ordered(rows.size());
    for (const Row& row:rows) ordered[ends[row.person]++]=row;
    rows.swap(ordered);
    size_t first=0;
    for (size_t last:ends) {
        std::sort(rows.begin()+first,rows.begin()+last,compare);
        first=last;
    }
}

class ActiveCampaigns {
    std::vector<unsigned> count,position;
public:
    std::vector<int> ids;
    explicit ActiveCampaigns(size_t n) : count(n),position(n) {}
    void add(int id) {
        if (!count[id]++) {position[id]=ids.size(); ids.push_back(id);}
    }
    void remove(int id) {
        if (!--count[id]) {
            int last=ids.back(); ids[position[id]]=last; position[last]=position[id]; ids.pop_back();
        }
    }
    void clear() {for (auto id:ids) count[id]=0; ids.clear();}
};

void process(size_t bucket, std::vector<EventBlock>& events_by_id,
             std::vector<Buckets<Click>>& clicks_by_file, const Dimensions& dim, const fs::path& output, const Keys& keys, CampaignTotals& campaigns) {
    Index persons;
    std::vector<Key> names;
    std::vector<IndexedEvent> events;
    size_t count=0;
    for (auto& block:events_by_id) count+=block.events[bucket].size();
    events.reserve(count);
    for (auto& block:events_by_id) {
        for (const auto& e:block.events[bucket]) if (e.sale.t!=invalid_time) {
            unsigned person=persons.insert(e.person);
            if (person==names.size()) names.push_back(e.person);
            events.push_back({person,e.sale});
        }
        std::vector<Event>().swap(block.events[bucket]);
    }
    std::vector<std::array<char,9>> decoded(names.size());
    std::vector<IndexedClick> clicks;
    for (auto& set:clicks_by_file) {
        for (const auto& c:set[bucket]) {
            unsigned person=persons.lookup(c.user);
            if (person!=UINT32_MAX) clicks.push_back({person,c.campaign,c.t,c.id});
        }
        std::vector<Click>().swap(set[bucket]);
    }
    order_by_person(events,names.size(),[](const auto& a,const auto& b){return a.sale.t<b.sale.t;});
    order_by_person(clicks,names.size(),[](const auto& a,const auto& b){
        if (a.t!=b.t) return a.t<b.t;
        return a.id>b.id;
    });

    daily_output::UserColumns users;
    std::unordered_map<int,int64_t> category_totals;
    ActiveCampaigns active(dim.campaign_names.size());
    // A bounded dense cache avoids millions of hash lookups for commonly
    // encountered days. Sparse or exceptionally wide datasets use the map.
    std::unordered_map<Millis,std::vector<Credit>> dense;
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
            users.append(keys.decode(names[person],decoded[person]),current_day,daily.sessions,daily.events,daily.revenue,top);
            daily={}; category_totals.clear();
        };
        while (e<events.size() && events[e].person==person) {
            Millis start=events[e].sale.t;
            Millis day=start/day_ms-(start%day_ms<0);
            if (day!=current_day) {finish_day(); current_day=day;}
            while (next<clicks.size() && clicks[next].person==person && clicks[next].t<=start) {
                active.add(clicks[next].campaign); ++next;
            }
            while (oldest<next && clicks[oldest].t<start-week_ms) {
                active.remove(clicks[oldest++].campaign);
            }
            int winner=oldest==next ? 0 : clicks[next-1].campaign;
            Credit* credit;
            auto day_it=dense.find(day);
            if (day_it==dense.end() && (dense.size()+1)*dim.campaign_names.size()<=65536)
                day_it=dense.try_emplace(day,dim.campaign_names.size()).first;
            if (day_it!=dense.end()) {
                auto& totals=day_it->second;
                for (int id:active.ids) ++totals[id].assists;
                credit=&totals[winner];
            } else {
                auto& totals=campaigns[day];
                for (int id:active.ids) ++totals[id].assists;
                credit=&totals[winner];
            }
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
            ++credit->sessions; credit->purchases+=revenue>0; credit->revenue+=revenue;
        }
        finish_day();
    }
    for (const auto& [day,totals]:dense) {
        auto& dest=campaigns[day];
        for (size_t id=0;id<totals.size();++id)
            if (totals[id].sessions || totals[id].assists) dest.emplace(id,totals[id]);
    }
    users.save(output/"user_daily"/("part-"+std::to_string(bucket)+".parquet"));
}

void save_campaigns(const std::vector<CampaignTotals>& partial, const Dimensions& dim, const fs::path& output) {
    CampaignTotals total;
    for (const auto& shard : partial) for (const auto& [day,campaigns] : shard) {
        auto& dest=total[day];
        for (const auto& [id,c] : campaigns) {
            auto& row=dest[id];
            row.sessions+=c.sessions; row.purchases+=c.purchases;
            row.revenue+=c.revenue; row.assists+=c.assists;
        }
    }
    daily_output::CampaignColumns ads;
    for (const auto& [day,campaigns] : total) for (const auto& [id,c] : campaigns)
        ads.append(dim.campaign_names[id],dim.channels[id],day,c.sessions,c.purchases,c.revenue,c.assists);
    ads.save(output/"campaign_daily"/"part-0.parquet");
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
        Keys users('u'), event_ids('e');
        Identities ids(input.chunks(data/"links"),users);
        auto ev=input.chunks(data/"events"), cl=input.chunks(data/"clicks");
        log("load identities and map files");
        std::vector<EventBlock> raw(ev.size());
        parallel(ev.size(),workers,[&](size_t i){read_events(ev[i],i,raw[i],dim,users,event_ids,ids,lo,hi);});
        log("parse events");
        parallel(shards,workers,[&](size_t i){dedup(i,raw);});
        log("deduplicate and resolve people");
        std::vector<Buckets<Click>> clicks(cl.size());
        parallel(cl.size(),workers,[&](size_t i){
            scan<4>(cl[i],[&](const auto& f){
                if (f[0].empty() || f[1].empty()) return;
                auto campaign=dim.campaigns.find(f[2]);
                Millis t=timestamp(f[3]);
                if (campaign==dim.campaigns.end() || t<lo-week_ms || t>=hi) return;
                Key person=ids.person(users.encode(f[1]));
                clicks[i][shard(person)].push_back({person,f[0],t,campaign->second});
            });
        });
        log("parse clicks");
        std::vector<CampaignTotals> partial(shards);
        parallel(shards,workers,[&](size_t i){process(i,raw,clicks,dim,scratch,users,partial[i]);});
        save_campaigns(partial,dim,scratch);
        log("session and attribution sweep");
    } catch (const std::exception& e) {std::cerr<<e.what()<<'\n';return 1;}
}
