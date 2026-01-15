#pragma once
#include "shared_types.h"
#include <cstdint>
#include <string>
#include <sstream>

namespace sketch {

class IvfBuilder;

class Centroids {
public:
    ~Centroids() { uninit(); }

    Ret init(const uint8_t* ptr, size_t memory_size);
    Ret init(const std::string& path);
    void uninit();

    size_t centroid_size() const { return centroid_size_; }
    size_t centroids_count() const { return size_; }
    const uint8_t* operator[](size_t index) const;
    const uint8_t* get_centroid(size_t index) const;

    static Ret write_centroids(const std::string& path, const IvfBuilder& builder);

    uint16_t find_nearest_centroid(const uint8_t* data, const DatasetType type, const uint16_t dim) const;
    void find_nearest_clusters(const uint8_t* data, const DatasetType type, const uint16_t dim,
            std::vector<uint16_t>& cluster_ids, uint64_t nprobes) const;

private:
    const uint8_t* ptr_ = nullptr;
    size_t memory_size_ = 0;
    size_t size_ = 0;
    size_t centroid_size_ = 0;
    bool mapped_file_ = false;

};

template <typename T>
void print_centroids(DatasetType type, size_t dim, size_t count, const T& centroids, std::stringstream& stream) {
    stream << std::endl;
    for (size_t i = 0; i < centroids.centroids_count() && i < count; i++) {
        switch (type) {
            case DatasetType::f32: {
                const float* f = reinterpret_cast<const float*>(centroids.get_centroid(i));
                for (size_t d = 0; d < dim && d < 4; d++) {
                    stream << f[d] << ", ";
                }
                stream << "\n";
                break;
            }
            case DatasetType::f16: {
                const float16_t* f = reinterpret_cast<const float16_t*>(centroids.get_centroid(i));
                for (size_t d = 0; d < dim && d < 4; d++) {
                    stream << f[d] << ", ";
                }
                stream << "\n";
                break;
            }
            case DatasetType::u8: {
                const uint8_t* f = reinterpret_cast<const uint8_t*>(centroids.get_centroid(i));
                for (size_t d = 0; d < dim && d < 4; d++) {
                    stream << static_cast<uint32_t>(f[d]) << ", ";
                }
                stream << "\n";
                break;
            }
        }
    }
}

} // namespace sketch