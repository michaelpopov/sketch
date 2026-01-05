#include "ivf_builder.h"
#include "math.h"
#include <stdio.h>
#include <sys/mman.h>
#include <cstring>
#include <cerrno>
#include <format>
#include <random>

namespace sketch {

/****************************************
 *
 *   Layout:
 *   |---------------+--------------------+----------+------------------+-----------------------|
 *         counts            records         sums           vectors A               vectors B                    
 */

IvfBuilder::IvfBuilder(DatasetType type, uint16_t dim, uint32_t centroids_count, uint32_t records_count)
    : type_(type),
      records_count_(records_count),
      centroids_count_(centroids_count),
      dim_(dim),
      size_(calc_size(type, dim, centroids_count, records_count)),
      vector_size_(calc_record_size(type, dim))
{

}

IvfBuilder::~IvfBuilder() {
    if (ptr_) {
        uninit();
    }
}

Ret IvfBuilder::init() {
    void* ptr = mmap(nullptr, size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (ptr == MAP_FAILED) {
        return std::format("Failed to allocate memory: {}", strerror(errno));
    }
    ptr_ = static_cast<uint8_t*>(ptr);

    counts_size_ = centroids_count_ * sizeof(uint32_t);
    records_size_ = records_count_ * sizeof(uint8_t*);
    sums_size_ = centroids_count_ * dim_ * sizeof(double);

    counts_ = reinterpret_cast<uint32_t*>(ptr_);
    records_ = reinterpret_cast<uint8_t**>(ptr_ + counts_size_);
    sums_ = reinterpret_cast<double*>(ptr_ + counts_size_ + records_size_);
    centroids_ = ptr_ + counts_size_ + records_size_ + sums_size_;
    centroids_size_ = centroids_count_ * vector_size_;

    return 0;
}

Ret IvfBuilder::uninit() {
    if (ptr_) {
        munmap(ptr_, size_);
        ptr_ = nullptr;
    }

    return 0;
}

// static
uint64_t IvfBuilder::calc_size(DatasetType type, uint16_t dim, uint32_t centroids_count, uint32_t records_count) {
    const uint64_t record_size = calc_record_size(type, dim);

    const uint64_t counts_size = centroids_count * sizeof(uint32_t);
    const uint32_t records_size = records_count * sizeof(uint8_t*);
    const uint32_t sums_size = centroids_count * dim * sizeof(double);
    const uint64_t vectors_size = centroids_count * record_size * 2;

    return counts_size + records_size + sums_size + vectors_size;
}

const uint8_t* IvfBuilder::get_centroid(size_t index) const {
    return get_centroids(current_set_type_) + index * vector_size_;
}

const uint8_t* IvfBuilder::get_centroids(SetType setType) const {
    switch (setType) {
        case SetType::First: return centroids_;
        case SetType::Second: return centroids_ + centroids_size_;
    }
    return nullptr;
}

Ret IvfBuilder::init_centroids_kmeans_plus_plus() {
    // 1. Pick first center randomly
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, records_count_ - 1);    

    const uint8_t* record = nullptr;
    const size_t attempts_count = records_count_;
    for (size_t i = 0; i < attempts_count; i++) {
        size_t random_index = dis(gen);
        if (records_[random_index] != nullptr) {
            record = records_[random_index];
            break;
        }
    }

    if (record == nullptr) {
        return "Failed to select initial centroid for KMeans++";
    }
    

    uint8_t* centroid = const_cast<uint8_t*>(get_centroid(0));
    memcpy(centroid, record, vector_size_);
    size_t centroids_count = 1;

    std::vector<double> distances_sq(records_count_, 0.0);

    while (centroids_count < centroids_count_) {
        double sum_sq = 0.0;

        // 2. Compute D(x)^2 for all points
        for (size_t j = 0; j < records_count_; j++) {
            uint8_t* p = records_[j];
            if (p == nullptr) {
                continue;
            }
            double min_dist_sq = std::numeric_limits<double>::max();

            for (size_t i = 0; i < centroids_count_; i++) {
                const uint8_t* c = get_centroid(i);
                double dist_sq = 0.0;
                switch (type_) {
                    case DatasetType::f32:
                        dist_sq = distance_L2_square((float*)p, (float*)c, dim_);
                        break;
                    case DatasetType::f16:
                        dist_sq = distance_L2_square((float16_t*)p, (float16_t*)c, dim_);
                        break;
                }

                min_dist_sq = std::min(min_dist_sq, dist_sq);
            }

            distances_sq[j] = min_dist_sq;
            sum_sq += min_dist_sq;
        }

        // 3. Select next center based on weighted probability
        std::uniform_real_distribution<> weight_dis(0, sum_sq);
        double threshold = weight_dis(gen);
        double cumulative_sum = 0.0;

        for (size_t i = 0; i < records_count_; ++i) {
            cumulative_sum += distances_sq[i];
            if (cumulative_sum >= threshold) {
                const auto& record = records_[i];
                uint8_t* centroid = const_cast<uint8_t*>(get_centroid(centroids_count));
                memcpy(centroid, record, vector_size_);
                centroids_count++;
                break;
            }
        }
    }
    
    return 0;
}

Ret IvfBuilder::recalc_centroids() {
    assert(current_set_type_ == SetType::First);

    auto ret = internal_recalc_centroids();
    if (ret != 0) {
        return ret;
    }

    current_set_type_ = SetType::Second;

    ret = internal_recalc_centroids();
    if (ret != 0) {
        return ret;
    }

    current_set_type_ = SetType::First;

    return 0;
}

Ret IvfBuilder::internal_recalc_centroids() {
    const uint8_t* current_centroids = get_centroids(current_set_type_);
    const uint8_t* next_centroids = get_centroids(current_set_type_ == SetType::First ? SetType::Second : SetType::First);

    memset(counts_, 0, counts_size_);
    memset(sums_, 0, sums_size_);

    uint32_t* counts = get_counts();

    for (size_t i = 0; i < records_count_; i++) {
        const auto& record = records_[i];
        if (record == nullptr) {
            continue;
        }

        size_t best_centroid_index = 0;
        double min_dist = std::numeric_limits<double>::max();

        for (size_t j = 0; j < centroids_count_; j++) {
            const auto& centroid = current_centroids + j * vector_size_;
            double dist = distance_L2_square(type_, record, centroid, dim_);
            if (dist < min_dist) {
                min_dist = dist;
                best_centroid_index = j;
            }
        }

        double* sums = sums_ + best_centroid_index * dim_;
        switch (type_) {
            case DatasetType::f32: apply_sum(reinterpret_cast<float*>(record), sums, dim_); break;
            case DatasetType::f16: apply_sum(reinterpret_cast<float16_t*>(record), sums, dim_); break;
        }
        counts[best_centroid_index]++;   
    }

    for (size_t j = 0; j < centroids_count_; j++) {
        uint8_t* centroid = const_cast<uint8_t*>(next_centroids + j * vector_size_);

        if (counts[j] == 0) {
            const auto& current_centroid = current_centroids + j * vector_size_;
            memcpy(centroid, current_centroid, vector_size_);
            continue;
        }

        const double* sums = sums_ + j * dim_;
        switch (type_) {
            case DatasetType::f32: apply_div(reinterpret_cast<float*>(centroid), sums, dim_, counts[j]); break;
            case DatasetType::f16: apply_div(reinterpret_cast<float16_t*>(centroid), sums, dim_, counts[j]); break;
        }
    }

    return 0;
}

} // namespace sketch
