#pragma once
#include "shared_types.h"
#include <cstdint>
#include <mutex>
#include <string>
#include <memory>
#include <vector>

namespace sketch {

class IvfBuilder {
public:
    IvfBuilder(DatasetType type, uint16_t dim, uint32_t centroids_count, uint32_t records_count);
    ~IvfBuilder();
    Ret init();
    Ret uninit();

    uint32_t records_count() const { return records_count_; }
    uint32_t centroids_count() const { return centroids_count_; }
    uint32_t centroids_size() const { return centroids_size_; }

    uint8_t* get_centroids();
    const uint8_t* get_centroid(size_t index) const;
    const uint8_t* get_record(size_t index) const { return records_[index]; }
    uint32_t* get_counts() { return reinterpret_cast<uint32_t*>(ptr_); }

    using RecordPtr = const uint8_t*;
    void set_record(size_t index, RecordPtr record_ptr) {
        if (index < records_count_) {
            records_[index] = record_ptr;
        }
    }

    Ret init_centroids_kmeans_plus_plus();
    Ret recalc_centroids();

private:
    enum class SetType { First, Second, };

private:
    const DatasetType type_;
    const uint32_t records_count_;
    const uint32_t centroids_count_;
    const uint16_t dim_;
    const uint64_t size_ = 0;
    const uint64_t vector_size_;
    
    uint8_t* ptr_ = nullptr;

    SetType current_set_type_ = SetType::First;

    uint32_t* counts_ = nullptr;
    RecordPtr* records_ = nullptr;
    double* sums_ = nullptr;
    uint8_t* centroids_ = nullptr;

    uint64_t counts_size_ = 0;
    uint64_t records_size_ = 0;
    uint64_t sums_size_ = 0;
    uint64_t centroids_size_ = 0;

private:
    static uint64_t calc_size(DatasetType type, uint16_t dim, uint32_t centroids_count, uint32_t records_count);
    const uint8_t* get_centroids(SetType setType) const;
    Ret internal_recalc_centroids();
};

} // namespace sketch
