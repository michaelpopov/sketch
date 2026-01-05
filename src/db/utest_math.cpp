#include "math.h"
#include "vector"
#include "log.h"
#include "gtest/gtest.h"

using namespace sketch;

TEST(MATH, L1) {
    size_t dim = 768;

    {
        std::vector<float> a(dim);
        std::vector<float> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        float dist = distance_L1(a.data(), b.data(), dim);
        ASSERT_FLOAT_EQ(dim, dist);
    }

    {
        std::vector<uint16_t> a(dim);
        std::vector<uint16_t> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        uint16_t dist = distance_L1(a.data(), b.data(), dim);
        ASSERT_EQ(dim, dist);
    }
}

TEST(MATH, L2) {
    size_t dim = 768;

    {
        std::vector<float> a(dim);
        std::vector<float> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        float dist = distance_L2(a.data(), b.data(), dim);
        ASSERT_FLOAT_EQ(std::sqrt(dim), dist);
    }

    {
        std::vector<uint16_t> a(dim);
        std::vector<uint16_t> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        auto dist = distance_L2(a.data(), b.data(), dim);
        ASSERT_FLOAT_EQ(std::sqrt(dim), dist);
    }
}

TEST(MATH, Cosine) {
    size_t dim = 768;

    {
        std::vector<float> a(dim);
        std::vector<float> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        float dist = distance_cos(a.data(), b.data(), dim);
        ASSERT_NEAR(1.0, dist, 0.001);
    }

    {
        std::vector<uint16_t> a(dim);
        std::vector<uint16_t> b(dim);
        for (size_t i = 0; i < dim; i++) {
            a[i] = i;
            b[i] = i + 1;
        }

        auto dist = distance_cos(a.data(), b.data(), dim);
        ASSERT_NEAR(1.0, dist, 0.001);
    }
}
