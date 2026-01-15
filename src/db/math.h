#pragma once
#include "shared_types.h"
#include <cstdint>
#include <cmath>
#include <iostream>

namespace sketch {

template <typename T>
__attribute__((simd))
double distance_L1(const T* a, const T* b, uint64_t dim) {
    double dist = 0.0;
    for (uint64_t i = 0; i < dim; i++) {
        dist += std::abs(a[i] - b[i]);
    }
    return dist;
}

template <typename T>
__attribute__((simd))
double distance_L2(const T* a, const T* b, uint64_t dim) {
    double dist = 0.0;
    for (uint64_t i = 0; i < dim; i++) {
        double diff = a[i] - b[i];
        dist += diff * diff;
    }
    return std::sqrt(dist);
}

template <typename T>
__attribute__((simd))
double distance_L2_square(const T* a, const T* b, uint64_t dim) {
    double dist = 0.0;
    for (uint64_t i = 0; i < dim; i++) {
        double diff = a[i] - b[i];
        dist += diff * diff;
    }
    return dist;
}

template <typename T>
__attribute__((simd))
double distance_cos(const T* a, const T* b, uint64_t dim) {
    double dot_product = 0.0;
    double a_norm = 0.0;
    double b_norm = 0.0;

    for (uint64_t i = 0; i < dim; i++) {
        dot_product += a[i] * b[i];
        a_norm += a[i] * b[i];
        b_norm += a[i] * b[i];
    }

    return dot_product / (std::sqrt(a_norm) * std::sqrt(b_norm));
}

__attribute__((simd))
static inline double distance_L2_square(DatasetType type, const uint8_t* a, const uint8_t* b, uint64_t dim) {
    switch (type) {
        case DatasetType::f32: 
            return distance_L2_square<float>(reinterpret_cast<const float*>(a), reinterpret_cast<const float*>(b), dim);
        case DatasetType::f16: 
            return distance_L2_square<float16_t>(reinterpret_cast<const float16_t*>(a), reinterpret_cast<const float16_t*>(b), dim);
        case DatasetType::u8: 
            return distance_L2_square<uint8_t>(reinterpret_cast<const uint8_t*>(a), reinterpret_cast<const uint8_t*>(b), dim);
    }
    return 0.0;
}

template <typename T>
__attribute__((simd))
void apply_div(T* a, const double* b, uint64_t dim, uint32_t div) {
    for (uint64_t i = 0; i < dim; i++) {
        a[i] = static_cast<T>(b[i] / div);
    }
}

template <typename T>
__attribute__((simd))
void apply_sum(const T* a, double* b, uint64_t dim) {
    for (uint64_t i = 0; i < dim; i++) {
        b[i] += a[i];
    }
}

template <typename T>
__attribute__((simd))
void calc_residual(const T* rec, const T* cent, T* residual, uint64_t dim) {
    for (uint64_t i = 0; i < dim; i++) {
        residual[i] = rec[i] - cent[i];
    }
}


} // namespace sketch
