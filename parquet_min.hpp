// parquet_min.hpp -- dependency-free Parquet writer (C++17, libc only).
// PLAIN encoding, UNCOMPRESSED, one row group, one data page per column.
// Supports REQUIRED/OPTIONAL columns of: UTF8 string, DATE (int32 days), int64.
// Checked with PyArrow and the official development-answer comparator.
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>
#include <stdexcept>
#include <fcntl.h>
#include <unistd.h>

namespace pq {
using Bytes = std::vector<uint8_t>;

struct Thrift {                       // Thrift compact protocol writer
    Bytes& o; int last = 0; std::vector<int> stack;
    explicit Thrift(Bytes& out) : o(out) {}
    void varint(uint64_t v) { while (v >= 0x80) { o.push_back(uint8_t(v) | 0x80); v >>= 7; } o.push_back(uint8_t(v)); }
    void zz(int64_t v) { varint((uint64_t(v) << 1) ^ uint64_t(v >> 63)); }
    void field(int id, int type) {
        int d = id - last;
        if (d > 0 && d <= 15) o.push_back(uint8_t(d << 4 | type)); else { o.push_back(uint8_t(type)); zz(id); }
        last = id;
    }
    void i32(int id, int32_t v) { field(id, 5); zz(v); }
    void i64(int id, int64_t v) { field(id, 6); zz(v); }
    void str(int id, std::string_view s) { field(id, 8); varint(s.size()); o.insert(o.end(), s.begin(), s.end()); }
    void begin(int id) { field(id, 12); stack.push_back(last); last = 0; }   // nested struct field
    void elem_begin() { stack.push_back(last); last = 0; }                    // struct inside a list
    void end() { o.push_back(0); last = stack.empty() ? 0 : stack.back(); if (!stack.empty()) stack.pop_back(); }
    void list(int id, int elem, size_t n) {
        field(id, 9);
        if (n < 15) o.push_back(uint8_t(n << 4 | elem)); else { o.push_back(uint8_t(0xF0 | elem)); varint(n); }
    }
};

enum Type { INT32 = 1, INT64 = 2, BYTE_ARRAY = 6 };
enum Conv { NONE = -1, UTF8 = 0, DATE = 6 };

struct Column {
    std::string name; int type, conv; bool optional; size_t rows;
    Bytes page;                        // page body (levels + values)
    int64_t values;                    // non-null values
};

class Table {
    std::vector<Column> cols; size_t rows_ = 0; bool have_rows = false;
    void check_rows(size_t n) {
        if (!have_rows) { rows_ = n; have_rows = true; }
        else if (rows_ != n) throw std::runtime_error("column length mismatch");
    }
    static void le32(Bytes& b, uint32_t v) { for (int i = 0; i < 4; ++i) b.push_back(uint8_t(v >> (8 * i))); }
public:
    // `present` (optional column): one 0/1 per row; `v` holds only the non-null values, in order.
    void add_str(std::string name, const std::vector<std::string_view>& v, const std::vector<uint8_t>* present = nullptr) {
        size_t rows = present ? present->size() : v.size(); check_rows(rows);
        Column c{std::move(name), BYTE_ARRAY, UTF8, present != nullptr, rows, {}, int64_t(v.size())};
        levels(c, present);
        size_t bytes = 0; for (auto s : v) bytes += 4 + s.size();
        c.page.reserve(c.page.size() + bytes);
        for (auto s : v) { le32(c.page, uint32_t(s.size())); c.page.insert(c.page.end(), s.begin(), s.end()); }
        cols.push_back(std::move(c));
    }
    void add_date(std::string name, const std::vector<int32_t>& v) { add_fixed(std::move(name), INT32, DATE, v.data(), v.size(), 4); }
    void add_i64(std::string name, const std::vector<int64_t>& v) { add_fixed(std::move(name), INT64, NONE, v.data(), v.size(), 8); }

    Bytes encode() const {
        Bytes out = {'P', 'A', 'R', '1'};
        struct Chunk { int64_t offset, size, values; };
        std::vector<Chunk> chunks;
        if (rows_) for (const auto& c : cols) {
            Bytes header; Thrift t(header);
            t.i32(1, 0);                                   // DATA_PAGE
            t.i32(2, int32_t(c.page.size())); t.i32(3, int32_t(c.page.size()));
            t.begin(5); t.i32(1, int32_t(c.rows)); t.i32(2, 0); t.i32(3, 3); t.i32(4, 3); t.end();
            t.end();
            chunks.push_back({int64_t(out.size()), int64_t(header.size() + c.page.size()), int64_t(c.rows)});
            out.insert(out.end(), header.begin(), header.end());
            out.insert(out.end(), c.page.begin(), c.page.end());
        }
        Bytes meta; Thrift t(meta);
        t.i32(1, 1);                                       // version
        t.list(2, 12, cols.size() + 1);                    // schema
        t.elem_begin(); t.str(4, "schema"); t.i32(5, int32_t(cols.size())); t.end();
        for (const auto& c : cols) {
            t.elem_begin();
            t.i32(1, c.type); t.i32(3, c.optional ? 1 : 0); t.str(4, c.name);
            if (c.conv != NONE) t.i32(6, c.conv);
            if (c.conv == UTF8) { t.begin(10); t.begin(1); t.end(); t.end(); }      // LogicalType.STRING
            if (c.conv == DATE) { t.begin(10); t.begin(6); t.end(); t.end(); }      // LogicalType.DATE
            t.end();
        }
        t.i64(3, int64_t(rows_));
        t.list(4, 12, rows_ ? 1 : 0);                      // row groups
        if (rows_) {
            int64_t total = 0; for (auto& ch : chunks) total += ch.size;
            t.elem_begin();
            t.list(1, 12, cols.size());
            for (size_t i = 0; i < cols.size(); ++i) {
                t.elem_begin();
                t.i64(2, chunks[i].offset);
                t.begin(3);
                t.i32(1, cols[i].type);
                t.list(2, 5, 2); t.zz(0); t.zz(3);          // encodings: PLAIN, RLE
                t.list(3, 8, 1); t.varint(cols[i].name.size()); meta.insert(meta.end(), cols[i].name.begin(), cols[i].name.end());
                t.i32(4, 0);                                // UNCOMPRESSED
                t.i64(5, chunks[i].values); t.i64(6, chunks[i].size); t.i64(7, chunks[i].size);
                t.i64(9, chunks[i].offset);
                t.end(); t.end();
            }
            t.i64(2, total); t.i64(3, int64_t(rows_));
            t.end();
        }
        t.str(6, "parquet_min");
        t.end();
        out.insert(out.end(), meta.begin(), meta.end());
        le32(out, uint32_t(meta.size()));
        out.insert(out.end(), {'P', 'A', 'R', '1'});
        return out;
    }

    // Writes to path + ".tmp", then renames: readers never see a partial file.
    void save(const std::string& path) const {
        std::string tmp = path + ".tmp";
        write_file(tmp);
        if (rename(tmp.c_str(), path.c_str()) != 0) throw std::runtime_error("publish failed");
    }

    void write_file(const std::string& tmp) const {
        Bytes b = encode();
        int fd = open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("cannot open " + tmp);
        for (size_t off = 0; off < b.size();) {
            ssize_t n = write(fd, b.data() + off, b.size() - off);
            if (n <= 0) { close(fd); throw std::runtime_error("write failed"); }
            off += size_t(n);
        }
        if (close(fd) != 0) throw std::runtime_error("close failed: " + tmp);
    }
private:
    static void levels(Column& c, const std::vector<uint8_t>* present) {
        if (!present) return;
        size_t groups = (present->size() + 7) / 8;
        Bytes rle; Thrift t(rle); t.varint((groups << 1) | 1);          // one bit-packed run, bit width 1
        size_t base = rle.size(); rle.resize(base + groups, 0);
        for (size_t i = 0; i < present->size(); ++i) if ((*present)[i]) rle[base + i / 8] |= uint8_t(1u << (i % 8));
        le32(c.page, uint32_t(rle.size())); c.page.insert(c.page.end(), rle.begin(), rle.end());
    }
    void add_fixed(std::string name, int type, int conv, const void* p, size_t n, size_t width) {
        check_rows(n);
        Column c{std::move(name), type, conv, false, n, {}, int64_t(n)};
        if (n) c.page.assign(static_cast<const uint8_t*>(p), static_cast<const uint8_t*>(p) + n * width);   // little-endian host
        cols.push_back(std::move(c));
    }
};
} // namespace pq
