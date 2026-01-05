#pragma once
#include "shared_types.h"
#include <atomic>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace sketch {

struct TextItem {
    size_t tag_offset;
    size_t data_offset;
};

struct TextView {
    std::string_view tag;
    std::string_view data;
};

class InputData {
public:
    ~InputData();
    int init(const std::string_view& path);
    int init(const char* ptr, size_t size);

    size_t count() const { return items_.size(); }
    std::optional<TextView> get(size_t index) const;
    int get(size_t index, const DatasetMetadata md, uint64_t& tag, std::vector<uint8_t>& vec) const;

    size_t size() const { return items_.size(); }

private:
    std::vector<TextItem> items_;
    const char* data_ = nullptr;
    size_t size_;
    bool mapped_ = false;

private:
    int load_items();

};

class InputDataGenerator {
public:
    static int generate(const std::string_view& path, size_t dim, size_t count, size_t start = 0);
};

} // namespace sketch