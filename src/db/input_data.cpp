#include "input_data.h"
#include "string_utils.h"
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <iostream>

namespace sketch {

/************************************************************************************
 *    InputDataGenerator
 */
int InputDataGenerator::generate(const std::string_view& path, size_t dim, size_t count, size_t start) {
    if (dim == 0 || count == 0) {
        return -1;
    }

    FILE* fp = fopen(std::string(path).c_str(), "w");
    if (!fp) {
        return -1;
    }

    char tag_buf[128];
    char buf[128];
    char last_buf[128];
    std::vector<char> line(1024*16);

    for (size_t i = 0; i < count; ++i) {
        size_t n = i + start;
        size_t line_len = 0;
        size_t last_len = 0;

        size_t tag_len = snprintf(tag_buf, sizeof(tag_buf), "%zu : [ ", n);
        line_len = snprintf(buf, sizeof(buf), "%zu.1, ", n);
        last_len = snprintf(last_buf, sizeof(last_buf), "%zu.1 ]\n", n);

        size_t offset = 0;
        memcpy(line.data() + offset, tag_buf, tag_len);
        offset += tag_len;

        for (size_t j = 0; j < dim - 1; ++j) {
            if (line.size() < offset + line_len) {
                line.resize(line.size() * 2);
            }
            memcpy(line.data() + offset, buf, line_len);
            offset += line_len;
        }

        if (line.size() < offset + last_len) {
            line.resize(line.size() + last_len);
        }
        memcpy(line.data() + offset, last_buf, last_len);
        offset += last_len;

        ssize_t written = fwrite(line.data(), 1, offset, fp);
        if (written != static_cast<ssize_t>(offset)) {
            fclose(fp);
            return -1;
        }
    }

    fclose(fp);
    return 0;
}

/************************************************************************************
 *    InputData
 */
InputData::~InputData() {
    if (data_ && mapped_) {
        munmap(const_cast<char*>(data_), size_);
    }
}

int InputData::init(const std::string_view& path) {
    int fd = open(std::string(path).c_str(), O_RDONLY);
    if (fd == -1) {
        return -1;
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        return -1;
    }
    size_ = sb.st_size;

    void* ptr = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        return -1;
    }

    data_ = static_cast<char*>(ptr);
    mapped_ = true;
    return load_items();
}

int InputData::init(const char* ptr, size_t size) {
    data_ = ptr;
    size_ = size;

    return load_items();
}

std::optional<TextView> InputData::get(size_t index) const {
    if (index >= items_.size()) {
        return std::nullopt;
    }

    const auto& item = items_[index];
    const char* tag_start = data_ + item.tag_offset;
    size_t tag_len = item.data_offset - item.tag_offset - 1;

    size_t data_len = 0;
    if (index == items_.size() - 1) {
        data_len = size_ - item.data_offset;
    } else {
        const auto& next_item = items_[index + 1];
        data_len = next_item.tag_offset - item.data_offset;
    }

    const char* data_start = data_ + item.data_offset;
    return TextView{std::string_view(tag_start, tag_len), std::string_view(data_start, data_len)};
}

int InputData::load_items() {
    if (!data_) {
        return -1;
    }

    size_t count = 0;
    size_t offset = 0;
    while (offset < size_) {
        const char* tag_end = findchr(data_ + offset, ':', size_);
        if (!tag_end) {
            break;
        }

        TextItem item {
            .tag_offset = offset,
            .data_offset = static_cast<size_t>(tag_end - data_ + 1),
        };

        items_.push_back(item);

        const char* data_end = findchr(data_ + item.data_offset, '\n', size_);
        if (!data_end) {
            break;
        }

        offset = data_end - data_ + 1;
        count++;
    }

    return offset == size_ ? 0 : -1;
}

int InputData::get(size_t index, const DatasetMetadata md, uint64_t& tag, std::vector<uint8_t>& vec) const {
    const auto opt = get(index);
    if (!opt) {
        return -1;
    }

    const auto& val = *opt;
    tag = u64_from_string_view(val.tag);

    switch (md.type) {
        case DatasetType::f32:
            vec.resize(sizeof(float) * md.dim);
            convert_vector_f32(val.data, vec);
            break;
        case DatasetType::f16:
            vec.resize(sizeof(float16_t) * md.dim);
            convert_vector_f16(val.data, vec);
            break;
        case DatasetType::u8:
            return -1; // Unsupported type
    }

    return 0;
}

} // namespace sketch