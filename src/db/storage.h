#pragma once
#include "shared_types.h"

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>

namespace sketch {

using GetResult = std::pair<Record, Ret>;
using PutResult = std::pair<uint64_t, Ret>;

enum class ScanResult {
    Finished,
    Deleted,
    Ok,
};

class Storage {
public:
    Storage(const std::string& path, uint64_t record_size);
    ~Storage();
    Ret create(uint64_t initial_count);
    Ret init();
    Ret uninit();

    GetResult get_record(uint64_t record_id);
    ScanResult scan_record(uint64_t record_id, Record& record);
    PutResult put_record(DataBuffer& data);
    Ret update_record(uint64_t record_id, const DataBuffer& data);
    Ret delete_record(uint64_t record_id);

    uint64_t records_count() const { return upper_record_id_ - deleted_records_.size(); }
    uint64_t upper_record_id() const { return upper_record_id_; }
    uint64_t records_limit() const { return records_limit_; }
    uint64_t deleted_count() const { return deleted_records_.size(); }

    bool is_deleted(uint32_t record_id) const {
        return deleted_records_.find(record_id) != deleted_records_.end();
    }

    uint8_t* get_record_data(uint32_t record_id) {
        if (record_id >= upper_record_id_) {
            return nullptr;
        }
        return reinterpret_cast<uint8_t*>(memmap_ + header_size_ + record_id * full_record_size_);
    }

private:
    std::string path_;
    const uint64_t record_size_;
    const uint64_t header_size_ = HeaderSize;
    const uint64_t full_record_size_;

    int fd_ = -1;
    char* memmap_ = nullptr;
    uint64_t mem_size_ = 0;

    uint64_t upper_record_id_ = 0;
    uint64_t records_limit_ = 0;
    std::unordered_set<uint32_t> deleted_records_;

private:
    Ret open_write_file();
    Ret map_read_memory();
    Ret write_data(uint64_t record_id, const uint8_t* data, uint64_t data_size);
    Ret write_data_at_offset(uint64_t offset, const uint8_t* data, uint64_t data_size);
    Ret read_info(bool& need_scan);
    Ret write_info();
    Ret scan();
};

} // namespace sketch