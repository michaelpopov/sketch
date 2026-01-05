#include "centroids.h"
#include "ivf_builder.h"
#include "math.h"
#include <fstream>
#include <iostream>
#include <format>
#include <queue>
#include <experimental/scope>
#include <stdio.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace sketch {

static constexpr uint64_t MagicNumber = 0xDEADBEEF;

Ret Centroids::init(const uint8_t* ptr, size_t memory_size) {
    if (!ptr) {
        return "Invalid centroids pointer";
    }

    if (memory_size < sizeof(uint64_t) * 3) {
        return "Invalid centroids memory buffer size";
    }

    const uint64_t* header = reinterpret_cast<const uint64_t*>(ptr);
    if (header[0] != MagicNumber) {
        return "Invalid centroids magic value";
    }

    centroid_size_ = header[1];
    size_ = header[2];

    size_t required_size = sizeof(uint64_t) * 3 + size_ * centroid_size_;
    if (memory_size < required_size) {
        return "Invalid centroids data size";
    }

    ptr_ = ptr;
    memory_size_ = memory_size;

    return 0;
}

Ret Centroids::init(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        return std::format("Failed to open file '{}'", path);
    }

    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        return std::format("Failed to get file size '{}'", path);
    }

    void* map = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (map == MAP_FAILED) {
        return std::format("Failed to map file '{}'", path);
    }

    Ret ret = init(static_cast<const uint8_t*>(map), sb.st_size);
    if (ret != 0) {
        munmap(map, sb.st_size);
        return ret;
    }

    mapped_file_ = true;
    return 0;
}

void Centroids::uninit() {
    if (mapped_file_ && ptr_) {
        munmap(const_cast<uint8_t*>(ptr_), memory_size_);
    }
    ptr_ = nullptr;
}

const uint8_t* Centroids::operator[](size_t index) const {
    return get_centroid(index);
}

const uint8_t* Centroids::get_centroid(size_t index) const {
    if (!ptr_ || index >= size_) {
        return nullptr;
    }
    return ptr_ + sizeof(uint64_t) * 3 + index * centroid_size_;
}

//static
Ret Centroids::write_centroids(const std::string& path, const IvfBuilder& builder) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) {
        return std::format("Failed to open file '{}' for writing", path);
    }
    const std::experimental::scope_exit closer([&] {
        fclose(f);
    });

    const uint64_t centroid_size = builder.centroids_size();
    const uint64_t count = builder.centroids_count();

    fwrite(&MagicNumber, sizeof(MagicNumber), 1, f);
    fwrite(&centroid_size, sizeof(centroid_size), 1, f);
    fwrite(&count, sizeof(count), 1, f);

    for (size_t i = 0; i < builder.centroids_count(); i++) {
        const uint8_t* c = builder.get_centroid(i);
        ssize_t ret = fwrite(c, centroid_size, 1, f);
        if (ret != 1) {
            return std::format("Failed to write centroid {} to file '{}'", i, path);
        }
    }

    fflush(f);

    return 0;
}

uint16_t Centroids::find_nearest_centroid(const uint8_t* data, const DatasetType type, const uint16_t dim) const {
    uint16_t nearest_centroid = 0;
    double min_dist = std::numeric_limits<double>::max();

    for (uint64_t i = 0; i < size_; i++) {
        double dist = distance_L2_square(type, data, get_centroid(i), dim);
        if (dist < min_dist) {
            min_dist = dist;
            nearest_centroid = i;
        }
    }

    return nearest_centroid;       
}

void Centroids::find_nearest_clusters(const uint8_t* data, const DatasetType type, const uint16_t dim,
        std::vector<uint16_t>& cluster_ids, uint64_t nprobes) const {

    cluster_ids.clear();
    std::priority_queue<DistItem> pq;

    for (uint64_t i = 0; i < size_; i++) {
        DistItem di;
        di.record_id = i;
        di.dist = distance_L2_square(type, data, get_centroid(i), dim);

        pq.push(di);
        if (pq.size() > nprobes) {
            pq.pop();
        }

    }

    while (!pq.empty()) {
        auto cid = pq.top().record_id;
        cluster_ids.push_back(cid);
        pq.pop();
    }
}

} // namespace sketch
