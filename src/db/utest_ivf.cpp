#include "engine.h"
#include "command_router.h"
#include "string_utils.h"
#include "log.h"
#include "gtest/gtest.h"

#include <unistd.h>
#include <filesystem>
#include <memory>
#include <iostream>
#include <fstream>
#include <format>
#include <experimental/scope>

using namespace sketch;

#ifdef CLOUD
static const char* GeneratedFile = "/mnt/data/test/generated_ivf.data";
static const char* Path = "/mnt/data/test";
#else
static const char* GeneratedFile = "/home/mpopov/test/generated_ivf.data";
static const char* Path = "/home/mpopov/test";
#endif

class DmlTestSettings {
public:
    DmlTestSettings(uint64_t dim = 128, uint64_t count = 8) {
        //TempLogLevel temp_level(LL_DEBUG);

        cfg_.data_path = path_;
        std::filesystem::remove_all(cfg_.data_path);
        mkdir(cfg_.data_path.c_str(), 0755);

        engine_ = std::make_unique<Engine>(cfg_);
        Ret ret = engine_->init();
        if (ret != 0)  std::cerr << ret.message() << std::endl;

        router_ = std::make_unique<CommandRouter>(*engine_);
        ret = router_->init();
        if (ret != 0)  std::cerr << ret.message() << std::endl;

        ret = router_->process_command("CREATE CATALOG test;");
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
        ret = router_->process_command(std::format("CREATE DATASET test.ds TYPE=f32 DIM={} NODES={};", dim, count));
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
        ret = router_->process_command("USE test.ds;");
        if (ret != 0)  std::cerr << "ERROR: " << ret.message() << std::endl;
    }

    ~DmlTestSettings() {
        engine_ = nullptr;
        std::filesystem::remove_all(cfg_.data_path);
    }

    Engine& engine() { return *engine_; }
    CommandRouter& router() { return *router_; }

private:
    const char* path_ = Path;
    Config cfg_;
    std::unique_ptr<Engine> engine_;
    std::unique_ptr<CommandRouter> router_;
};

TEST(IVF, SamplingTest) {
    const uint64_t test_data_start_from = 1;
    const uint64_t centroids_count = 4;
    const uint64_t dim = 8;
    const uint64_t nodes = 4;
    const uint64_t data_count = 100'000;
    const uint64_t sample_count = 10'000;

    DmlTestSettings dts(dim, nodes);
    CommandRouter& router = dts.router();

    auto cmd = std::format("GENERATE {} {} {} {}", GeneratedFile, data_count, dim, test_data_start_from);
    auto ret = router.process_command(cmd);
    std::experimental::scope_exit closer([&] {
        unlink(GeneratedFile);
    });
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    cmd = std::format("LOAD {}", GeneratedFile);
    ret = router.process_command(cmd);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    cmd = std::format("MAKE_IVF {} {} {}", centroids_count, sample_count, 16);
    ret = router.process_command(cmd);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    double avg = 0.0;
    std::vector<uint64_t> buckets(10);

    auto test_func = [&avg, &buckets] (DatasetType, uint64_t dim, uint64_t count, const uint8_t* data) -> Ret {
        const float* vectors = reinterpret_cast<const float*>(data);
        double sum = 0.0;
        for (uint64_t i = 0; i < count; i++) {
            const float* vector = vectors + i * dim;
            float val = vector[0];
            sum += val;
            uint64_t bucket = (uint64_t)(val) / 10'000UL;
            if (bucket >= buckets.size()) {
                bucket = buckets.size() - 1;
            }
            buckets[bucket]++;
        }
        avg = sum / count;
        return 0;
    };

    auto ds = dts.router().dcp().current_dataset();

    ds->set_make_residuals_test_func(test_func);
    ret = ds->make_residuals(sample_count);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    const uint64_t target_avg = 50'000;
    const uint64_t acceptable_deviation = 10'000;
    ASSERT_TRUE(target_avg - acceptable_deviation < avg &&
                 avg < target_avg + acceptable_deviation);

    const uint64_t target_bucket_size = 1000;
    const uint64_t acceptable_bucket_deviation = 100;
    for (size_t i = 0; i < buckets.size(); i++) {
        ASSERT_TRUE(target_bucket_size - acceptable_bucket_deviation < buckets[i] &&
                    buckets[i] < target_bucket_size + acceptable_bucket_deviation);
    }

    //std::cerr << "Avg: " << avg << std::endl;
    //for (size_t i = 0; i < buckets.size(); i++) {
    //    std::cerr << "Bucket " << i << ": " << buckets[i] << std::endl;
    //}
}

/*static void print_data(DatasetType type, uint64_t dim, uint64_t count, const uint8_t* data, std::ostream& stream) {
    switch (type) {
        case DatasetType::f32: {
            const float* f = reinterpret_cast<const float*>(data);
            for (uint64_t i = 0; i < dim && i < count; i++) {
                stream << f[i] << ", ";
            }
            break;
        }
        case DatasetType::f16: {
            const float16_t* f16 = reinterpret_cast<const float16_t*>(data);
            for (uint64_t i = 0; i < dim && i < count; i++) {
                stream << f16[i] << ", ";
            }
            break;
        }
        case DatasetType::u8: {
            for (uint64_t i = 0; i < dim && i < count; i++) {
                stream << (int)data[i] << ", ";
            }
            break;
        }
    }
}*/

void prepare_input_data(const char* path, uint64_t data_count, uint64_t dim, uint64_t centroids_count) {
    float val1 = 1.1;
    float val2 = 5.5;

    FILE* f = fopen(path, "w");
    for (uint64_t i = 0; i < data_count; i++) {
        fprintf(f, "%lu : [ ", i + 1);

        size_t half = dim / 2;
        for (size_t j = 0; j < half; j++) {
            fprintf(f, "%.2f, ", val1);
        }
        for (size_t j = half; j < dim-1; j++) {
            fprintf(f, "%.2f, ", val2);
        }
        fprintf(f, "%.2f ]\n", val2);

        val1 += 1.0;
        val2 += 1.0;

        if (val1 > centroids_count + 1.0) {
            val1 = 1.1;
            val2 = 5.5;
        }
    }
    fclose(f);
}

TEST(IVF, PqTest) {
    const uint64_t centroids_count = 4;
    const uint64_t dim = 8;
    const uint64_t nodes = 1; // TODO: 4; !!!!
    const uint64_t data_count = 100'000;
    const uint64_t sample_count = 10'000;
    const uint64_t chunk_count = 2;
    const uint64_t pq_centroids_depth = 4;

    DmlTestSettings dts(dim, nodes);

    prepare_input_data(GeneratedFile, data_count, dim, centroids_count);

    auto ds = dts.router().dcp().current_dataset();

    LoadReport report;
    auto ret = ds->load(GeneratedFile, report);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    TempLogLevel temp_level(LL_DEBUG);

    auto test_func0 = [&] (const std::unique_ptr<Centroids>& centroids) -> Ret {
        (void)centroids;
        /*for (size_t i = 0; i < centroids->centroids_count(); i++) {
            print_data(DatasetType::f32, dim, dim, centroids->get_centroid(i), std::cerr);
            std::cerr << std::endl;
        }*/
        return 0;
    };

    ds->set_mock_ivf_test_func(test_func0);
    ret = ds->mock_ivf(centroids_count, sample_count, chunk_count, pq_centroids_depth);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    auto test_func1 = [] (DatasetType, uint64_t, uint64_t, const uint8_t*) -> Ret {
        return 0;
    };

    ds->set_make_residuals_test_func(test_func1);
    ret = ds->make_residuals(sample_count);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    uint64_t result_pq_centroids_count = 0;
    auto test_func2 = [&] (const std::vector<std::unique_ptr<Centroids>>& pq_centroids) -> Ret {
        result_pq_centroids_count = pq_centroids.size();

        /*std::cerr << ">>>>> PQ centroids >>>>>>>" << std::endl;
        for (const auto& centroids : pq_centroids) {
            std::cerr << "Count: " << centroids->centroids_count() << std::endl;
            for (size_t i = 0; i < centroids->centroids_count(); i++) {
                print_data(DatasetType::f32, dim, dim, centroids->get_centroid(i), std::cerr);
                std::cerr << std::endl;
            }
        }
        std::cerr << std::endl;*/

        return 0;
    };

    ds->set_make_pq_centroids_test_func(test_func2);
    ret = ds->make_pq_centroids(chunk_count, pq_centroids_depth);
    ASSERT_EQ(0, ret) << "ERROR: " << ret.message();

    ASSERT_EQ(chunk_count, result_pq_centroids_count);
}
