// Read-only input mappings own the bytes referenced by all downstream string views.
#pragma once
#include <algorithm>
#include <array>
#include <atomic>
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
void scan(Text bytes, Visitor visit) {
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

// Store short names inline and use the full mapped bytes for longer names.
// Length is part of the key, including when the name contains zero bytes.
class NameIndex {
    struct Slot {
        uint64_t head = 0;
        uint32_t tail = 0;
        uint32_t size = 0;
        unsigned id = 0;
    };
    std::vector<Slot> table;
    std::unordered_map<Text,unsigned> long_names;
    size_t occupied = 0;
    static Slot pack(Text s) {
        Slot key;
        key.size = s.size();
        std::memcpy(&key.head, s.data(), std::min<size_t>(8,s.size()));
        if (s.size()>8) std::memcpy(&key.tail,s.data()+8,s.size()-8);
        return key;
    }
    size_t position(const Slot& key) const {
        uint64_t h = key.head ^ (uint64_t(key.tail) * 0x9e3779b97f4a7c15ULL);
        h ^= h >> 33;
        h *= 0xff51afd7ed558ccdULL;
        h ^= h >> 33;
        size_t i = h & (table.size()-1);
        while (table[i].size && (table[i].head!=key.head || table[i].tail!=key.tail || table[i].size!=key.size))
            i=(i+1)&(table.size()-1);
        return i;
    }
    void grow() {
        std::vector<Slot> old;
        old.swap(table);
        table.resize(std::max<size_t>(32,old.size()*2));
        for (const auto& slot : old) if (slot.size) table[position(slot)]=slot;
    }
public:
    NameIndex() {grow();}
    unsigned insert(Text name, unsigned proposed) {
        if (name.size()>12) return long_names.emplace(name,proposed).first->second;
        if ((occupied+1)*10 > table.size()*7) grow();
        Slot key=pack(name);
        auto& slot=table[position(key)];
        if (!slot.size) {key.id=proposed; slot=key; ++occupied;}
        return slot.id;
    }
    unsigned lookup(Text name) const {
        if (name.size()>12) {
            auto it=long_names.find(name);
            return it==long_names.end() ? UINT32_MAX : it->second;
        }
        const auto& slot=table[position(pack(name))];
        return slot.size ? slot.id : UINT32_MAX;
    }
};

class Identities {
    NameIndex index;
    std::vector<unsigned> parent, weight;
    std::vector<Text> smallest;
    unsigned add(Text id) {
        unsigned i=index.insert(id,parent.size());
        if (i==parent.size()) { parent.push_back(i); weight.push_back(1); smallest.push_back(id); }
        return i;
    }
    unsigned root(unsigned i) {
        while (i!=parent[i]) {parent[i]=parent[parent[i]]; i=parent[i];}
        return i;
    }
public:
    explicit Identities(const std::vector<Text>& chunks) {
        for (Text block : chunks) scan<2>(block,[&](const auto& f) {
            if (f[0].empty() || f[1].empty()) return;
            unsigned a=root(add(f[0])), b=root(add(f[1]));
            if (a==b) return;
            if (weight[a]<weight[b]) std::swap(a,b);
            parent[b]=a; weight[a]+=weight[b]; smallest[a]=std::min(smallest[a],smallest[b]);
        });
        for (unsigned i=0; i<parent.size(); ++i) parent[i]=root(i);
    }
    Text person(Text id) const {
        unsigned i=index.lookup(id);
        return i==UINT32_MAX ? id : smallest[parent[i]];
    }
};
