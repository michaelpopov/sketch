#include "storage.h"
#include "log.h"
#include <format>
#include <experimental/scope>
#include <iostream>

#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

namespace sketch {

static constexpr uint64_t INVALID_TAG = UINT64_MAX;
static constexpr uint64_t DELETED_TAG = UINT64_MAX - 1;


Storage::Storage(const std::string& path, uint64_t record_size)
  : path_(path),
    record_size_(record_size),
    full_record_size_(record_size + header_size_)
{
}

Storage::~Storage() {
    if (memmap_) {
        munmap(memmap_, mem_size_);
    }
    if (fd_ != -1) {
        close(fd_);
    }
}

/****************************************************************************
 *  Storage initialization
 */

 Ret Storage::create(uint64_t initial_count) {
    {
        int fd = open(path_.c_str(), O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            const auto err_msg = std::format("Failed to create data file at '{}': {}", path_, strerror(errno));
            LOG_ERROR << err_msg;
            return err_msg;
        }

        std::experimental::scope_exit closer([&] {
            close(fd);
        });

        uint64_t total_size = header_size_ + initial_count * full_record_size_;
        if (ftruncate(fd, total_size) != 0) {
            const auto err_msg = std::format("Failed to set size of data file at '{}' to {}: {}", path_, total_size, strerror(errno));
            LOG_ERROR << err_msg;
            return err_msg;
        }
    }

    std::experimental::scope_exit closer([&] {
        close(fd_);
        fd_ = -1;
    });

    auto ret = open_write_file();
    if (ret != 0) {
        return ret; 
    }
    
    uint64_t invalid_tag = INVALID_TAG;
    ret = write_data(0, reinterpret_cast<uint8_t*>(&invalid_tag), sizeof(invalid_tag));
    if (ret != 0) {
        return ret;
    }

    return 0;
}

Ret Storage::Storage::init() {
    auto ret = open_write_file();
    if (ret != 0) {
        return ret; 
    }
    ret = map_read_memory();
    if (ret != 0) {
        return ret; 
    }

    bool need_scan = false;
    ret = read_info(need_scan);
    if (ret != 0) {
        return ret; 
    }

    if (need_scan) {
        ret = scan();
        if (ret != 0) {
            return ret; 
        }
    }

    return 0;
}

Ret Storage::uninit() {
    return write_info();
}

Ret Storage::open_write_file() {
    fd_ = open(path_.c_str(), O_RDWR, S_IRUSR | S_IWUSR);
    if (fd_ < 0) {
        const auto err_msg = std::format("Failed to open data file at '{}' for wrting: ", path_, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    return 0;
}

Ret Storage::map_read_memory() {
    int fd = open(path_.c_str(), O_RDONLY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        const auto err_msg = std::format("Failed to open data file at '{}' for reading: ", path_, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    std::experimental::scope_exit closer([&] {
        close(fd);
    });

    struct stat file_stat;
    if (fstat(fd, &file_stat) < 0) {
        const auto err_msg = std::format("Failed to stat data file at '{}': {}", path_, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    mem_size_ = file_stat.st_size;
    if (mem_size_ == 0 || (mem_size_ - header_size_) % full_record_size_ != 0) {
        const auto err_msg = std::format("Data file at '{}' has invalid size {}", path_, mem_size_);
        LOG_ERROR << err_msg;
        return err_msg;
    }

    memmap_ = (char*)mmap(nullptr, mem_size_, PROT_READ, MAP_SHARED, fd, 0);
    if (memmap_ == MAP_FAILED) {
        const auto err_msg = std::format("Failed to mmap data file at '{}': {}", path_, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    records_limit_ = (mem_size_ - header_size_) / full_record_size_;

    return 0;
}

Ret Storage::read_info(bool& need_scan) {
    const std::string info_path = path_ + ".info";
    FILE* f = fopen(info_path.c_str(), "r");
    if (!f) {
        if (errno == ENOENT) {
            need_scan = true;
            return 0;
        } else {
            const auto err_msg = std::format("Failed to open info file at '{}': {}", info_path, strerror(errno));
            LOG_ERROR << err_msg;
            return err_msg;
        }
    }

    unlink(info_path.c_str());

    std::experimental::scope_exit closer([&] {
        fclose(f);
    });

    char line[256];
    if (fgets(line, sizeof(line), f) == nullptr) {
        if (feof(f)) {
            return 0; // Empty file is ok.
        }
        const auto err_msg = std::format("Failed to read from info file at '{}': {}", info_path, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    try {
        upper_record_id_ = std::stoull(line);
    } catch (const std::exception& e) {
        const auto err_msg = std::format("Invalid upper_record_id value in info file at '{}': {}", info_path, e.what());
        LOG_ERROR << err_msg;
        return err_msg;
    }

    while (fgets(line, sizeof(line), f) != nullptr) {
        try {
            deleted_records_.insert(std::stoul(line));
        } catch (const std::exception& e) {
            const auto err_msg = std::format("Invalid deleted_record value in info file at '{}': {}", info_path, e.what());
            LOG_ERROR << err_msg;
            return err_msg;
        }
    }

    if (ferror(f)) {
        const auto err_msg = std::format("Error while reading from info file at '{}': {}", info_path, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    return 0;
}

Ret Storage::write_info() {
    const std::string info_path = path_ + ".info";
    const std::string info_path_temp = info_path + ".tmp";
    FILE* f = fopen(info_path_temp.c_str(), "w");
    if (!f) {
        const auto err_msg = std::format("Failed to open info file at '{}' for writing: {}", info_path_temp, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    std::experimental::scope_exit closer([&] {
        if (f) {
            fclose(f);
            f = nullptr;
            unlink(info_path_temp.c_str());
        }
    });

    if (fprintf(f, "%lu\n", upper_record_id_) < 0) {
        const auto err_msg = std::format("Failed to write upper_record_id to info file at '{}': {}", info_path, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    for (const auto& deleted_record : deleted_records_) {
        if (fprintf(f, "%u\n", deleted_record) < 0) {
            const auto err_msg = std::format("Failed to write deleted_record to info file at '{}': {}", info_path, strerror(errno));
            LOG_ERROR << err_msg;
            return err_msg;
        }
    }

    if (fflush(f) != 0) {
        const auto err_msg = std::format("Failed to flush info file at '{}': {}", info_path_temp, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    fclose(f);
    f = nullptr;

    if (rename(info_path_temp.c_str(), info_path.c_str()) != 0) {
        const auto err_msg = std::format("Failed to rename info file from '{}' to '{}': {}", info_path_temp, info_path, strerror(errno));
        LOG_ERROR << err_msg;
        return err_msg;
    }

    return 0;
}

Ret Storage::scan() {
    for (uint64_t index = 0; index < records_limit_; index++) {
        uint64_t offset = index * full_record_size_;

        uint64_t tag = *reinterpret_cast<uint64_t*>(memmap_ + offset);

        if (tag == DELETED_TAG) {
            deleted_records_.insert(index);
        } else if (tag == INVALID_TAG) {
            upper_record_id_ = index;
            break;
        }
    }

    return 0;
}

/****************************************************************************
 *  Data manipuation
 */

Ret Storage::write_data(uint64_t record_id, const uint8_t* data, uint64_t data_size) {
    const uint64_t offset = record_id * full_record_size_;
    return write_data_at_offset(offset, data, data_size);
}

Ret Storage::write_data_at_offset(uint64_t offset, const uint8_t* data, uint64_t data_size) {
    ssize_t overall_written = 0;
    while (overall_written < static_cast<ssize_t>(data_size)) {
        ssize_t bytes_written = pwrite(fd_, data + overall_written, data_size - overall_written, offset + overall_written);
        if (bytes_written < 0 && errno != EINTR) {
            const auto err_msg = std::format("Failed to write data at offset {} in file '{}': {}", offset + overall_written, path_, strerror(errno));
            LOG_ERROR << err_msg;
            return err_msg;
        }
        overall_written += bytes_written;
    }

    return 0;
}

GetResult Storage::get_record(uint64_t record_id) {
    Record record;

    if (record_id >= upper_record_id_) {
        return std::make_pair<>(record, Ret(std::format("Record ID {} out of range in storage at '{}'", record_id, path_)));
    }

    const uint64_t offset = record_id * full_record_size_;

    record.tag = *reinterpret_cast<uint64_t*>(memmap_ + offset);
    record.data = reinterpret_cast<uint8_t*>(memmap_ + offset + header_size_);

    if (record.tag == INVALID_TAG || record.tag == DELETED_TAG) {
        return std::make_pair<>(record, Ret("Invalid record."));
    }

    return std::make_pair<>(record, Ret(0));
}

ScanResult Storage::scan_record(uint64_t record_id, Record& record) {
    if (record_id >= upper_record_id_) {
        return ScanResult::Finished;
    }

    const uint64_t offset = record_id * full_record_size_;
    record.tag = *reinterpret_cast<uint64_t*>(memmap_ + offset);

    if (record.tag == INVALID_TAG) {
        return ScanResult::Finished;
    }

    if (record.tag == DELETED_TAG) {
        return ScanResult::Deleted;
    }

    record.data = reinterpret_cast<uint8_t*>(memmap_ + offset + header_size_);

    return ScanResult::Ok;
}

Ret Storage::delete_record(uint64_t record_id) {
    if (record_id >= upper_record_id_) {
        return std::format("Record ID {} out of range in storage at '{}'", record_id, path_);
    }

    uint64_t deleted_tag = DELETED_TAG;
    auto ret = write_data(record_id, reinterpret_cast<const uint8_t*>(&deleted_tag), sizeof(deleted_tag));
    if (ret != 0) {
        return ret;
    }

    deleted_records_.insert(record_id);

    return 0;
}

PutResult Storage::put_record(DataBuffer& data) {
    uint64_t record_id = UINT64_MAX;

    if (data.record_size() > record_size_ || data.header_size() != header_size_) {
        return std::make_pair<>(UINT64_MAX, std::format("Invalid data size {} for record in storage at '{}'", data.record_size(), path_));
    }

    if (!deleted_records_.empty()) {
        const auto iter = deleted_records_.begin();
        record_id = *iter;

        // No need to write the following record header to mark end of recrods.
        auto ret = write_data(record_id, data.const_data_ptr(), data.record_size() + data.header_size());
        if (ret != 0) {
            return std::make_pair<>(UINT64_MAX, ret);
        }

        deleted_records_.erase(iter);
        return std::make_pair<>(record_id, 0);
    }

    // TODO: Implement dynamic resizing of storage file.
    if (upper_record_id_ >= records_limit_) {
        return std::make_pair<>(UINT64_MAX, std::format("No space left to insert new record in storage at '{}'", path_));
    }

    data.set_footer(INVALID_TAG); // Mark end of records.

    record_id = upper_record_id_;
    auto ret = write_data(record_id, data.const_data_ptr(), data.size());
    if (ret != 0) {
        return std::make_pair<>(UINT64_MAX, ret);
    }

    upper_record_id_++;
    return std::make_pair<>(record_id, 0);
}

Ret Storage::update_record(uint64_t record_id, const DataBuffer& data) {
    if (data.record_size() > record_size_ || data.header_size() != header_size_) {
        return std::format("Invalid data size {} for record in storage at '{}'", data.record_size(), path_);
    }

    if (record_id >= upper_record_id_) {
        return std::format("Record ID {} out of range in storage at '{}'", record_id, path_);
    }

    if (data.record_size() != record_size_) {
        return std::format("Invalid data size {} for record in storage at '{}'", data.size(), path_);
    }

    uint64_t offset = record_id * full_record_size_;
    uint64_t tag = 0;
    memcpy(&tag, memmap_ + offset, sizeof(tag));
    if (tag == DELETED_TAG || tag == INVALID_TAG) {
        return std::format("Cannot update deleted or invalid record ID {} in storage at '{}'", record_id, path_);
    }

    auto ret = write_data_at_offset(offset + header_size_, data.const_record_ptr(), data.record_size());
    if (ret != 0) {
        return ret;
    }

    return 0;
}

} // namespace sketch