// Batch complete daily rows directly into Parquet column chunks.
#pragma once
#include "native_input.hpp"
#include <arrow/io/file.h>
#include <parquet/column_writer.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

namespace daily_output {
using namespace parquet;
using Node = schema::NodePtr;
inline Node field(const char* name, Type::type type, ConvertedType::type logical=ConvertedType::NONE,
                  Repetition::type repetition=Repetition::REQUIRED) {
    return schema::PrimitiveNode::Make(name,repetition,type,logical);
}

class File {
    std::shared_ptr<::arrow::io::FileOutputStream> stream;
    std::unique_ptr<ParquetFileWriter> writer;
public:
    File(const fs::path& path, const schema::NodeVector& fields) {
        PARQUET_ASSIGN_OR_THROW(stream, ::arrow::io::FileOutputStream::Open(path.string()));
        auto schema=std::static_pointer_cast<schema::GroupNode>(
            schema::GroupNode::Make("daily",Repetition::REQUIRED,fields));
        auto properties=WriterProperties::Builder().disable_statistics()->compression(Compression::SNAPPY)->build();
        writer=ParquetFileWriter::Open(stream,schema,properties);
    }
    RowGroupWriter* group() {return writer->AppendRowGroup();}
    void close() {writer->Close(); PARQUET_THROW_NOT_OK(stream->Close());}
};

template<class Column, class T>
void write(RowGroupWriter* group, const std::vector<T>& values) {
    static_cast<Column*>(group->NextColumn())->WriteBatch(values.size(),nullptr,nullptr,values.data());
}

inline ByteArray bytes(Text text) {
    return ByteArray(static_cast<uint32_t>(text.size()),reinterpret_cast<const uint8_t*>(text.data()));
}

struct UserColumns {
    std::vector<ByteArray> names, categories;
    std::vector<int16_t> present;
    std::vector<int32_t> days;
    std::vector<int64_t> sessions, events, revenues;
    void append(Text name, int32_t day, int64_t ns, int64_t ne, int64_t revenue, Text top) {
        names.push_back(bytes(name)); days.push_back(day); sessions.push_back(ns);
        events.push_back(ne); revenues.push_back(revenue); present.push_back(revenue>0);
        if (revenue>0) categories.push_back(bytes(top));
    }
    void save(const fs::path& path) {
        File f(path,{field("user_id",Type::BYTE_ARRAY,ConvertedType::UTF8),
            field("day",Type::INT32,ConvertedType::DATE),field("sessions",Type::INT64),
            field("events",Type::INT64),field("revenue_cents",Type::INT64),
            field("top_category",Type::BYTE_ARRAY,ConvertedType::UTF8,Repetition::OPTIONAL)});
        if (!days.empty()) {
            auto g=f.group();
            write<ByteArrayWriter>(g,names); write<Int32Writer>(g,days); write<Int64Writer>(g,sessions);
            write<Int64Writer>(g,events); write<Int64Writer>(g,revenues);
            static_cast<ByteArrayWriter*>(g->NextColumn())->WriteBatch(days.size(),present.data(),nullptr,categories.data());
            g->Close();
        }
        f.close();
    }
};

struct CampaignColumns {
    std::vector<ByteArray> ids, channels;
    std::vector<int32_t> days;
    std::vector<int64_t> sessions, purchases, revenues, assists;
    void append(Text id,Text channel,int32_t day,int64_t ns,int64_t np,int64_t revenue,int64_t na) {
        ids.push_back(bytes(id)); channels.push_back(bytes(channel)); days.push_back(day);
        sessions.push_back(ns); purchases.push_back(np); revenues.push_back(revenue); assists.push_back(na);
    }
    void save(const fs::path& path) {
        File f(path,{field("campaign_id",Type::BYTE_ARRAY,ConvertedType::UTF8),
            field("channel",Type::BYTE_ARRAY,ConvertedType::UTF8),field("day",Type::INT32,ConvertedType::DATE),
            field("sessions",Type::INT64),field("purchasing_sessions",Type::INT64),
            field("revenue_cents",Type::INT64),field("assisted_sessions",Type::INT64)});
        if (!days.empty()) {
            auto g=f.group(); write<ByteArrayWriter>(g,ids); write<ByteArrayWriter>(g,channels);
            write<Int32Writer>(g,days); write<Int64Writer>(g,sessions); write<Int64Writer>(g,purchases);
            write<Int64Writer>(g,revenues); write<Int64Writer>(g,assists); g->Close();
        }
        f.close();
    }
};
}
