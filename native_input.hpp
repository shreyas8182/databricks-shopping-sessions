// Read-only input mappings own the bytes referenced by all downstream string views.
#pragma once
#include <algorithm>
#include <array>
#include <atomic>
#include <cstdlib>
#include <cstdio>
#include <sys/resource.h>
#include <charconv>
#include <chrono>
#include <cstring>
#include <exception>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

namespace fs = std::filesystem;
using Text = std::string_view;
using Millis = int64_t;
constexpr Millis day_ms = 86400000;
constexpr Millis week_ms = 7 * day_ms;
constexpr Millis invalid_time = std::numeric_limits<Millis>::min();

class Mapping {
    void* address = MAP_FAILED;
    size_t size = 0;
public:
    Mapping(const Mapping&) = delete;
    explicit Mapping(const fs::path& file) {
        int fd = open(file.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("cannot open " + file.string());
        struct stat info{};
        if (fstat(fd, &info) != 0) { close(fd); throw std::runtime_error("stat failed"); }
        size = info.st_size;
        if (size) address = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (size && address == MAP_FAILED) throw std::runtime_error("mmap failed");
    }
    ~Mapping() { if (size) munmap(address, size); }
    Text bytes() const { return size ? Text(static_cast<const char*>(address), size) : Text{}; }
};

class Inputs {
    std::vector<std::unique_ptr<Mapping>> owners;
public:
    std::vector<Text> chunks(const fs::path& path) {
        std::vector<fs::path> files;
        if (fs::is_directory(path)) {
            for (const auto& entry : fs::directory_iterator(path))
                if (entry.path().extension() == ".csv") files.push_back(entry.path());
        } else files.push_back(path);
        std::vector<Text> result;
        for (const auto& file : files) {
            owners.push_back(std::make_unique<Mapping>(file));
            Text bytes = owners.back()->bytes();
            size_t header = bytes.find('\n');
            if (header == Text::npos) continue;
            bytes.remove_prefix(header + 1);
            constexpr size_t target = 2 * 1024 * 1024;
            while (!bytes.empty()) {
                size_t end = bytes.find('\n', std::min(target, bytes.size()));
                size_t length = end == Text::npos ? bytes.size() : end + 1;
                result.push_back(bytes.substr(0, length));
                bytes.remove_prefix(length);
            }
        }
        return result;
    }
};

template<size_t Width, class Visitor>
void scan_scalar(Text bytes, Visitor visit) {
    std::array<Text, Width> fields;
    while (!bytes.empty()) {
        size_t newline = bytes.find('\n');
        Text row = bytes.substr(0, newline);
        bytes.remove_prefix(row.size() + (newline != Text::npos));
        for (size_t i = 0; i < Width; ++i) {
            size_t comma = row.find(',');
            if ((comma == Text::npos) != (i + 1 == Width)) break;
            Text field = row.substr(0, comma);
            if (field.size() >= 2 && field.front() == '"' && field.back() == '"') {
                field.remove_prefix(1); field.remove_suffix(1);
            }
            if (field.find('"') != Text::npos) break;
            fields[i] = field;
            if (comma == Text::npos) visit(fields);
            else row.remove_prefix(comma + 1);
        }
    }
}

// Produce one bit per byte for delimiters, LF and quotes. Each 16-byte
// load stays within the mapped input. The scalar path handles the final row.
#if defined(__x86_64__)
#include <immintrin.h>
#elif defined(__aarch64__)
#include <arm_neon.h>
#endif

struct CsvMasks {unsigned delimiters, newlines, quotes;};
#if defined(__aarch64__)
inline unsigned byte_mask(uint8x16_t bits) {
    const uint8x16_t weights={1,2,4,8,16,32,64,128,1,2,4,8,16,32,64,128};
    auto weighted=vandq_u8(bits,weights);
    return vaddv_u8(vget_low_u8(weighted)) | (unsigned(vaddv_u8(vget_high_u8(weighted)))<<8);
}
#endif
#if defined(__x86_64__) || defined(__aarch64__)
inline CsvMasks csv_masks(const char* p) {
#if defined(__x86_64__)
    auto v=_mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
    unsigned lf=_mm_movemask_epi8(_mm_cmpeq_epi8(v,_mm_set1_epi8(10)));
    unsigned comma=_mm_movemask_epi8(_mm_cmpeq_epi8(v,_mm_set1_epi8(',')));
    unsigned quotes=_mm_movemask_epi8(_mm_cmpeq_epi8(v,_mm_set1_epi8('"')));
#else
    auto v=vld1q_u8(reinterpret_cast<const uint8_t*>(p));
    unsigned lf=byte_mask(vceqq_u8(v,vdupq_n_u8(10)));
    unsigned comma=byte_mask(vceqq_u8(v,vdupq_n_u8(',')));
    unsigned quotes=byte_mask(vceqq_u8(v,vdupq_n_u8('"')));
#endif
    return {lf|comma,lf,quotes};
}
#endif

struct LineMasks {uint64_t separators=0,newlines=0,quotes=0;};
#if defined(__x86_64__) || defined(__aarch64__)
inline LineMasks line_masks(const char* p) {
    LineMasks m;
#if defined(__AVX2__)
    for (unsigned i=0;i<64;i+=32) {
        auto v=_mm256_loadu_si256(reinterpret_cast<const __m256i*>(p+i));
        uint32_t lf=_mm256_movemask_epi8(_mm256_cmpeq_epi8(v,_mm256_set1_epi8(10)));
        uint32_t comma=_mm256_movemask_epi8(_mm256_cmpeq_epi8(v,_mm256_set1_epi8(',')));
        uint32_t quotes=_mm256_movemask_epi8(_mm256_cmpeq_epi8(v,_mm256_set1_epi8('"')));
        m.separators |= uint64_t(lf|comma)<<i;
        m.newlines |= uint64_t(lf)<<i;
        m.quotes |= uint64_t(quotes)<<i;
    }
#else
    for (unsigned i=0;i<64;i+=16) {
        auto v=csv_masks(p+i);
        m.separators |= uint64_t(v.delimiters)<<i;
        m.newlines |= uint64_t(v.newlines)<<i;
        m.quotes |= uint64_t(v.quotes)<<i;
    }
#endif
    return m;
}
#endif
template<size_t Width,class Visitor>
void scan(Text bytes, Visitor visit) {
#if defined(__x86_64__) || defined(__aarch64__)
    std::array<Text,Width> fields;
    while (bytes.size()>=128) {
        const char* p=bytes.data();
        auto low=line_masks(p);
        LineMasks high;
        size_t offset=0;
        if (!low.newlines) {
            high=line_masks(p+64); offset=64;
            if (!high.newlines) {
                size_t n=bytes.find('\n',128);
                n=n==Text::npos ? bytes.size() : n+1;
                scan_scalar<Width>(bytes.substr(0,n),visit); bytes.remove_prefix(n); continue;
            }
        }
        auto& last=offset ? high : low;
        uint64_t lf=last.newlines & -last.newlines;
        last.quotes &= lf-1; last.separators &= lf|(lf-1);
        size_t n=offset+__builtin_ctzll(lf);
        bytes.remove_prefix(n+1);
        if (__builtin_popcountll(low.separators)+__builtin_popcountll(high.separators)!=Width) continue;
        // Every quote must be adjacent to a delimiter or the start of the row.
        uint64_t allowed=(low.separators<<1)|(low.separators>>1)|1|(high.separators<<63);
        if (low.quotes & ~allowed) continue;
        allowed=(high.separators<<1)|(high.separators>>1)|(low.separators>>63);
        if (high.quotes & ~allowed) continue;
        size_t start=0;
        bool valid=true;
        for (size_t i=0;i<Width;++i) {
            size_t end;
            if (low.separators) {end=__builtin_ctzll(low.separators);low.separators&=low.separators-1;}
            else {end=64+__builtin_ctzll(high.separators);high.separators&=high.separators-1;}
            size_t length=end-start;
            bool quoted=p[start]=='"';
            if ((length && (p[end-1]=='"')!=quoted) || (quoted && length<2)) {valid=false;break;}
            fields[i]=Text(p+start+quoted,length-2*quoted);
            start=end+1;
        }
        if (valid) visit(fields);
    }
#endif
    scan_scalar<Width>(bytes,visit);
}

template<class Task>
void parallel(size_t count, unsigned workers, Task task) {
    std::atomic<size_t> next{0};
    std::mutex error_lock;
    std::exception_ptr error;
    std::vector<std::thread> pool;
    for (size_t i = 0; i < std::min<size_t>(workers, count); ++i)
        pool.emplace_back([&] {
            try {
                for (size_t k; (k = next.fetch_add(1)) < count;) task(k);
            } catch (...) {
                std::lock_guard<std::mutex> lock(error_lock);
                if (!error) error = std::current_exception();
                next.store(count);
            }
        });
    for (auto& thread : pool) thread.join();
    if (error) std::rethrow_exception(error);
}

inline int pair_digits(Text s, size_t pos) {
    unsigned a = unsigned(s[pos] - '0'), b = unsigned(s[pos+1] - '0');
    return (a < 10 && b < 10) ? int(10*a+b) : -1;
}

inline Millis decimal(Text s) {
    if (s.empty() || s.front() < '0' || s.front() > '9') return invalid_time;
    Millis n;
    auto r = std::from_chars(s.data(), s.data()+s.size(), n);
    return r.ec == std::errc{} && r.ptr == s.data()+s.size() ? n : invalid_time;
}

inline Millis timestamp(Text s) {
    if ((s.size() != 20 && s.size() != 25) || s[4] != '-') return decimal(s);
    if (s[7]!='-' || s[10]!='T' || s[13]!=':' || s[16]!=':') return invalid_time;
    int century=pair_digits(s,0), yy=pair_digits(s,2), m=pair_digits(s,5), d=pair_digits(s,8);
    int h=pair_digits(s,11), min=pair_digits(s,14), sec=pair_digits(s,17);
    if (century<0 || yy<0 || m<1 || m>12 || h<0 || h>23 || min<0 || min>59 || sec<0 || sec>59)
        return invalid_time;
    int y=century*100+yy;
    const bool leap=(y%4==0 && (y%100!=0 || y%400==0));
    constexpr int lengths[]={31,28,31,30,31,30,31,31,30,31,30,31};
    if (!y || d<1 || d>lengths[m-1]+(m==2 && leap)) return invalid_time;
    int offset=0;
    if (s.size()==20) { if (s[19]!='Z') return invalid_time; }
    else {
        int oh=pair_digits(s,20), om=pair_digits(s,23);
        if ((s[19]!='+' && s[19]!='-') || s[22]!=':' || oh<0 || oh>23 || om<0 || om>59) return invalid_time;
        offset=(oh*60+om)*(s[19]=='+' ? 1 : -1);
    }
    // Count Gregorian days since 1970 using March as the first month.
    y -= m<=2;
    int era=y/400, year=y-era*400, month=m+(m>2 ? -3 : 9);
    int days=era*146097+year*365+year/4-year/100+(153*month+2)/5+d-1-719468;
    return days*day_ms+((h*60+min-offset)*60+sec)*1000LL;
}

using Key = uint64_t;
inline uint64_t key_hash(Key key) {
    key ^= key >> 31;
    key *= 0x9e3779b97f4a7c15ULL;
    return key ^ (key >> 33);
}

// Ordinary short ASCII suffixes fit in one word. All exceptional byte strings
// are interned by their complete value in a disjoint key space.
struct Profile {
    bool timing = enabled("SS_TIMING"), count = enabled("SS_COUNT_KEYS");
    std::atomic<uint64_t> event{0}, user{0};
    static bool enabled(const char* name) {
        const char* value = std::getenv(name);
        return value && std::string_view(value) == "1";
    }
    ~Profile() {
        if (count) std::fprintf(stderr, "slow_keys %llu event %llu user %llu\n",
            (unsigned long long)(event.load()+user.load()), (unsigned long long)event.load(),
            (unsigned long long)user.load());
        if (!timing) return;
        rusage usage{};
        if (getrusage(RUSAGE_SELF, &usage)) return;
        auto seconds = [](timeval t) { return t.tv_sec + t.tv_usec / 1e6; };
#ifdef __APPLE__
        constexpr long units = 1024 * 1024;
#else
        constexpr long units = 1024;
#endif
        std::fprintf(stderr, "user %.3f sys %.3f minflt %ld majflt %ld maxrss %ld MiB\n",
            seconds(usage.ru_utime), seconds(usage.ru_stime), usage.ru_minflt, usage.ru_majflt,
            usage.ru_maxrss / units);
    }
};
inline Profile profile;

class Keys {
    struct Extra {
        std::mutex lock;
        struct Slot { size_t hash=0; size_t position=0; };
        std::vector<Slot> slots=std::vector<Slot>(32);
        std::vector<Text> bytes;

        size_t insert(Text value, size_t hash) {
            size_t mask=slots.size()-1;
            size_t i=(hash>>6)&mask;
            while (slots[i].position) {
                if (slots[i].hash==hash && bytes[slots[i].position-1]==value)
                    return slots[i].position-1;
                i=(i+1)&mask;
            }
            bytes.push_back(value);
            slots[i]={hash,bytes.size()};
            if (bytes.size()*10>slots.size()*7) {
                std::vector<Slot> grown(slots.size()*2);
                mask=grown.size()-1;
                for (auto slot:slots) if (slot.position) {
                    size_t j=(slot.hash>>6)&mask;
                    while (grown[j].position) j=(j+1)&mask;
                    grown[j]=slot;
                }
                slots.swap(grown);
            }
            return bytes.size()-1;
        }
    };
    std::array<Extra,64> extras;
    char prefix;
    static constexpr Key indirect=1ULL<<63;
public:
    explicit Keys(char prefix) : prefix(prefix) {}
    Key encode(Text s) {
        if (s.size()>=2 && s.size()<=9 && s.front()==prefix) {
            size_t n=s.size()-1;
            Key word=0;
            if (n==8) std::memcpy(&word,s.data()+1,8);
            else std::memcpy(&word,s.data()+1,n);
            constexpr Key high=0x8080808080808080ULL;
            Key padded=word;
            if (n<8) padded |= (~Key{0})<<(8*n);
            if (!(word&high) && !((padded-0x0101010101010101ULL)&~padded&high)) return word;
        }
        if (profile.count) (prefix=='e' ? profile.event : profile.user).fetch_add(1, std::memory_order_relaxed);
        size_t hash=std::hash<Text>{}(s);
        size_t lane=hash%extras.size();
        auto& extra=extras[lane];
        std::lock_guard<std::mutex> lock(extra.lock);
        return indirect | (extra.insert(s,hash)<<6) | lane;
    }
    // Call after all encoders have joined. The caller owns short-key storage.
    Text decode(Key k, std::array<char,9>& buf) const {
        if (k&indirect) return extras[k&63].bytes[(k&~indirect)>>6];
        buf[0]=prefix;
        std::memcpy(buf.data()+1,&k,8);
        return Text(buf.data(),1+(64-__builtin_clzll(k)+7)/8);
    }
};

class Index {
    struct Slot {Key key=0; Key id=0;};
    std::vector<Slot> slots;
    unsigned count=0;
    size_t position(Key k) const {
        size_t i=key_hash(k)&(slots.size()-1);
        while (slots[i].key && slots[i].key!=k) i=(i+1)&(slots.size()-1);
        return i;
    }
    void resize(size_t n) {
        std::vector<Slot> old(n);
        slots.swap(old);
        for (auto v : old) if (v.key) slots[position(v.key)]=v;
    }
public:
    explicit Index(size_t hint=0) {
        size_t n=32; while(n<hint+hint/2) n*=2;
        slots.resize(n);
    }
    unsigned insert(Key k) {
        if ((count+1)*10>slots.size()*7) resize(slots.size()*2);
        auto& slot=slots[position(k)];
        if (!slot.key) slot={k,count++};
        return slot.id;
    }
    unsigned lookup(Key k) const {
        auto slot=slots[position(k)];
        return slot.key ? unsigned(slot.id) : UINT32_MAX;
    }
    void replace_ids(const std::vector<Key>& values) {
        for (auto& slot:slots) if (slot.key) slot.id=values[slot.id];
    }
    Key resolve(Key k) const {
        auto slot=slots[position(k)];
        return slot.key ? slot.id : k;
    }
};

class Identities {
    Index index;
    std::vector<unsigned> parent,weight;
    std::vector<Text> smallest;
    std::vector<Key> canonical;
    unsigned add(Key key, Text name) {
        unsigned i=index.insert(key);
        if (i==parent.size()) {
            parent.push_back(i); weight.push_back(1); smallest.push_back(name); canonical.push_back(key);
        }
        return i;
    }
    unsigned root(unsigned i) {
        while (i!=parent[i]) {parent[i]=parent[parent[i]]; i=parent[i];}
        return i;
    }
    static size_t hint(const std::vector<Text>& blocks) {
        size_t total=0; for (auto b:blocks) total+=b.size();
        return std::min<size_t>(total/16,4000000);
    }
public:
    Identities(const std::vector<Text>& blocks, Keys& names) : index(hint(blocks)) {
        for (Text block:blocks) scan<2>(block,[&](const auto& f){
            if (f[0].empty() || f[1].empty()) return;
            unsigned a=root(add(names.encode(f[0]),f[0])), b=root(add(names.encode(f[1]),f[1]));
            if (a==b) return;
            if (weight[a]<weight[b]) std::swap(a,b);
            parent[b]=a; weight[a]+=weight[b];
            if (smallest[b]<smallest[a]) {smallest[a]=smallest[b]; canonical[a]=canonical[b];}
        });
        std::vector<Text> group_names;
        for (unsigned i=0;i<parent.size();++i) if (parent[i]==i) {
            canonical[i]=group_names.size();
            group_names.push_back(smallest[i]);
        }
        for (unsigned i=0;i<parent.size();++i) canonical[i]=canonical[root(i)];
        index.replace_ids(canonical);
        smallest.swap(group_names);
        std::vector<Key>().swap(canonical);
        std::vector<unsigned>().swap(weight);
        std::vector<unsigned>().swap(parent);
    }
    unsigned lookup(Key key) const {return index.lookup(key);}
    size_t size() const {return smallest.size();}
    Text name(unsigned person) const {return smallest[person];}

};
