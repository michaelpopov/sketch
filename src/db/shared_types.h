#pragma once
#include <cassert>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <string.h>

namespace sketch {

using Commands = std::vector<std::string_view>;
using CommandNames = std::unordered_set<std::string_view>;
using Properties = std::unordered_map<std::string, std::string>;

struct DistItem {
    double dist = 0.0;
    uint64_t record_id = 0;
    uint64_t tag = 0;

    bool operator<(const DistItem& other) const {
        return dist < other.dist;
    }
};

enum class KnnType {
    Undefined,
    L1,
    L2,
    COS
};

enum class DatasetType {
    f16,
    f32,
};

#ifdef ARM64_ARCH
#define float16_t __fp16
#else
#define float16_t float
#endif

static constexpr uint64_t HeaderSize = sizeof(uint64_t);

static inline uint64_t calc_record_size(DatasetType type, size_t dim) {
    uint64_t record_size = 0;
    switch (type) {
        case DatasetType::f32: record_size = dim * sizeof(float); break;
        case DatasetType::f16: record_size = dim * sizeof(float16_t); break;
    }

    constexpr size_t alignment = sizeof(uint64_t);
    record_size = (record_size + alignment - 1) & ~(alignment - 1);

    return record_size;
}

struct DatasetMetadata {
    DatasetType type = DatasetType::f32;
    size_t dim = 1024;
    size_t nodes_count = 1;
    size_t index_id = 0;
    size_t pq_count = 0;
    uint64_t record_size() const {
        return calc_record_size(type, dim);
    }
};

struct Record {
    uint64_t tag = 0;
    uint8_t* data = nullptr;
};

class Ret {
public:
    Ret(int code) : code_(code) {}
    Ret(const std::string& message) : code_(-1), message_(message) {}
    Ret(const char* message) : code_(-1), message_(message) {}
    Ret(int code, const std::string& message, bool is_content = false)
      : code_(code), message_(message), is_content_(is_content) {}
    operator int() const { return code_; } // Automatic conversion to int
    int code() const { return code_; }
    const std::string& message() const { return message_; }
    bool is_content() const { return is_content_; }
    
private:
    int code_ = 0;
    std::string message_;
    bool is_content_ = false;
};

#define CHECK(ret) \
    if (ret != 0) { \
        return ret; \
    }

class DataBuffer {
public:
    DataBuffer(size_t record_size, size_t header_size) : header_size_(header_size) {
        assert(record_size > 0);
        assert(header_size >= sizeof(uint64_t));
        buffer_.resize(header_size * 2 + record_size);
    }

    DataBuffer(const DataBuffer&) = delete;
    DataBuffer& operator=(const DataBuffer&) = delete;

    void set_header(const uint64_t value) {
        assert(header_size_ >= sizeof(uint64_t));
        uint64_t* header = reinterpret_cast<uint64_t*>(header_ptr());
        *header = value;
    }

    uint64_t get_header() {
        assert(header_size_ >= sizeof(uint64_t));
        uint64_t header;
        memcpy(&header, header_ptr(), sizeof(uint64_t));
        return header;
    }   

    void set_footer(const uint64_t value) {
        assert(header_size_ >= sizeof(uint64_t));
        uint64_t* footer = reinterpret_cast<uint64_t*>(footer_ptr());
        *footer = value;
    }

    /*uint8_t* data_ptr() {
        return buffer_.data();
    }*/

    uint8_t* header_ptr() {
        return buffer_.data();
    }

    uint8_t* record_ptr() {
        return buffer_.data() + header_size_;
    }

    const uint8_t* const_record_ptr() const {
        return buffer_.data() + header_size_;
    }

    const uint8_t* const_data_ptr() const {
        return buffer_.data();
    }

    uint8_t* footer_ptr() {
        return buffer_.data() + buffer_.size() - header_size_;
    }

    size_t size() const {
        return buffer_.size();
    }

    size_t record_size() const {
        return buffer_.size() - 2 * header_size_;
    }

    size_t header_size() const {
        return header_size_;
    }

private:
    const size_t header_size_;
    std::vector<uint8_t> buffer_;
};
} // namespace sketch
