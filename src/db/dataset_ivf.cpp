#include "dataset.h"
#include "centroids.h"
#include "ivf_builder.h"
#include "math.h"
#include "string_utils.h"
#include "input_data.h"
#include "thread_pool.h"
#include "log.h"

#include <algorithm>
#include <experimental/scope>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <sstream>

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

namespace sketch {

Ret Dataset::sample_records(IvfBuilder& builder, ThreadPool* thread_pool) {
    uint64_t per_node_count = builder.records_count() / nodes_.size();
    if (per_node_count * nodes_.size() != builder.records_count()) {
        per_node_count += 1;
    }

    uint32_t from = 0;
    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), &builder, from, per_node_count] {
                return node_ptr->sample_records(builder, from, per_node_count);
            }));

            from += per_node_count;
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto _ = futures[node_index].get();
        }

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto _ = node->sample_records(builder, from, per_node_count);
            from += per_node_count;
        }
    }

    return 0;
}

Ret Dataset::init_centroids_kmeans_plus_plus(IvfBuilder& builder, ThreadPool* thread_pool) {
    auto ret = sample_records(builder, thread_pool);
    if (ret != 0) {
        return ret;
    }

    ret = builder.init_centroids_kmeans_plus_plus();
    if (ret != 0) {
        return ret;
    }

    std::stringstream sstream;
    print_centroids(metadata_.type, metadata_.dim, 16, builder, sstream);

    return Ret(0, sstream.str(), true);
}

Ret Dataset::write_index(IvfBuilder& builder, ThreadPool* thread_pool) {
    auto ret = write_centroids(builder);
    builder.uninit();
    if (ret != 0) {
        return ret;
    }

    ret = write_index_internal(thread_pool);
    if (ret != 0) {
        return ret;
    }

    return update_and_write_metadata();
}

Ret Dataset::write_centroids(IvfBuilder& builder) {
    const uint64_t next_index_id = metadata_.index_id + 1;
    const std::string index_path = path_ + "/index_" + std::to_string(next_index_id);
    if (!std::filesystem::create_directory(index_path)) {
        return std::format("Failed to create index directory {}", index_path);
    }

    const std::string centroids_path = index_path + "/centroids";
    return Centroids::write_centroids(centroids_path, builder);
}

Ret Dataset::write_index_internal(ThreadPool* thread_pool) {
    const InUseMarker in_use_marker(in_use_count_);

    const uint64_t next_index_id = metadata_.index_id + 1;
    const std::string index_path = path_ + "/index_" + std::to_string(next_index_id);
    const std::string centroids_path = index_path + "/centroids";
    if (!std::filesystem::exists(centroids_path)) {
        return "Centroids file does not exist";
    }

    auto centroids = std::make_unique<Centroids>();
    auto ret = centroids->init(centroids_path);
    if (ret != 0) {
        return ret;
    }

    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), &centroids, next_index_id] {
                return node_ptr->write_index(*centroids, next_index_id);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret res = futures[node_index].get();
            if (res != 0) {
                LOG_DEBUG << "ERROR: " << res.message();                
                ret = res;
            }
        }

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            Ret res = node->write_index(*centroids, next_index_id);
            if (res != 0) {
                ret = res;
            }
        }
    }

    return ret;
}

Ret Dataset::update_and_write_metadata() {
    metadata_.index_id++;
    auto ret = write_metadata();
    if (ret != 0) {
        return ret;
    }

    for (auto& node : nodes_) {
        node->uninit();
        node.reset();
    }

    const std::string index_path = path_ + "/index_" + std::to_string(metadata_.index_id);
    const std::string centroids_path = index_path + "/centroids";
    centroids_ = std::make_unique<Centroids>();
    ret = centroids_->init(centroids_path);
    if (ret != 0) {
        return ret;
    }

    std::stringstream sstream;
    print_centroids(metadata_.type, metadata_.dim, 16, *centroids_, sstream);

    return Ret(0, sstream.str(), true);
}

static void print_data(DatasetType type, uint64_t dim, uint64_t count, const uint8_t* data, std::ostream& stream) {
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
}

Ret Dataset::dump_ivf() {
    READ_OP_HEADER

    if (!centroids_) {
        return "Centroids not initialized";
    };

    std::stringstream sstream;

    sstream << "===== Centroids: ====\n";
    print_centroids(metadata_.type, metadata_.dim, centroids_->centroids_count(), *centroids_, sstream);

    const uint64_t max_residuals_to_print = 16;
    const std::string residuals_path = path_ + "/index_" + std::to_string(metadata_.index_id) + "/residuals";
    if (std::filesystem::exists(residuals_path)) {
        sstream << "\n";
        sstream << "Residuals:\n";
        const uint64_t record_size = metadata_.record_size();
        std::ifstream residuals_file(residuals_path, std::ios::binary);
        if (!residuals_file.is_open()) {
            return std::format("Failed to open residuals file at '{}'", residuals_path);
        }

        std::vector<uint8_t> record_data(record_size);
        uint64_t record_index = 0;
        while (true && record_index < max_residuals_to_print) {
            residuals_file.read(reinterpret_cast<char*>(record_data.data()), record_size);
            if (residuals_file.eof()) {
                break;
            }

            sstream << "  Residual " << record_index << ": ";
            print_data(metadata_.type, metadata_.dim, 4, record_data.data(), sstream);
            sstream << "\n";
            record_index++;
        }

    }

    sstream << "\n";
    sstream << "PQ Centroids:\n";
    for (size_t pq_index = 0; pq_index < pq_centroids_.size(); pq_index++) {
        sstream << "  PQ Chunk " << pq_index << ":\n";
        print_centroids(
            metadata_.type,
            metadata_.dim / pq_centroids_.size(),
            std::min(8UL, pq_centroids_[pq_index]->centroids_count()),
            *pq_centroids_[pq_index],
            sstream);
        sstream << "\n";
    }
    sstream << "\n";

    return Ret(0, sstream.str(), true);
}

Ret Dataset::make_residuals(uint64_t count, ThreadPool* thread_pool, MakeResidualsTestFunc test_func) {
    READ_OP_HEADER

    if (!centroids_) {
        return "Centroids not initialized";
    };

    if (count % centroids_->centroids_count() != 0) {
        count = ((count / centroids_->centroids_count()) + 1) * centroids_->centroids_count();
    }

    if (count % nodes_.size() != 0) {
        count = ((count / nodes_.size()) + 1) * nodes_.size();
    }

    const std::string index_path = path_ + "/index_" + std::to_string(metadata_.index_id);
    if (!std::filesystem::exists(index_path)) {
        std::filesystem::create_directory(index_path);
    }
    const std::string residuals_path = index_path + "/residuals";
    const uint64_t record_size = metadata_.record_size();
    const uint64_t residuals_file_size = record_size * count;

    int fd = open(residuals_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd == -1) {
        return std::format("Failed to create residuals file at '{}'", residuals_path);
    }

    ftruncate(fd, residuals_file_size);
    void* mapped = mmap(nullptr, residuals_file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (mapped == MAP_FAILED) {
        return std::format("Failed to mmap residuals file at '{}'", residuals_path);
    }

    const std::experimental::scope_exit unmapper([&] {
        munmap(mapped, residuals_file_size);
    });

    uint8_t* mapped_u8 = reinterpret_cast<uint8_t*>(mapped);
    auto per_node_count = count / nodes_.size();
    bool is_test_run = test_func != nullptr;

    Ret res{0};
    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), cents = centroids_.get(), per_node_count, mapped_u8, is_test_run] {
                return node_ptr->make_residuals(*cents, mapped_u8, per_node_count, is_test_run);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto ret = futures[node_index].get();
            if (ret != 0) {
                res = ret;
            }
        }

    } else {

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto ret = node->make_residuals(*centroids_, mapped_u8, per_node_count, is_test_run);
            if (ret != 0) {
                return ret;
            }
        }
    }

    if (res == 0 && test_func) {
        res = test_func(metadata_.type, metadata_.dim, count, mapped_u8);
    }

    return res;
}

class PQCentroidWorker {
public:
    PQCentroidWorker(
        const DatasetType _type,
        const uint64_t _record_size,
        const uint64_t _pq_centroids_record_size,
        const uint64_t _pq_centroid_dim,
        const uint64_t _pq_centroids_count,
        const uint64_t _records_count,
        const uint8_t* _residuals_mapped_u8,
        const std::string& _index_path)
    : type(_type)
    , record_size(_record_size)
    , pq_centroids_record_size(_pq_centroids_record_size)
    , pq_centroid_dim(_pq_centroid_dim)
    , pq_centroids_count(_pq_centroids_count)
    , records_count(_records_count)
    , residuals_mapped_u8(_residuals_mapped_u8)
    , index_path(_index_path)
    {}

    Ret build_pq_centroids(uint64_t pq_index) const {
        /*std::cerr << "Building PQ centroids for chunk " << pq_index << " ..." << std::endl;
        std::cerr << "Record size: " << record_size << std::endl;
        std::cerr << "PQ centroids record size: " << pq_centroids_record_size << std::endl;
        std::cerr << "PQ centroid dim: " << pq_centroid_dim << std::endl;
        std::cerr << "PQ centroids count: " << pq_centroids_count << std::endl;
        std::cerr << "Records count: " << records_count << std::endl;
        std::cerr << std::endl;*/

        IvfBuilder pq_builder(type, pq_centroid_dim, pq_centroids_count, records_count);
        auto ret = pq_builder.init();
        CHECK(ret)

        for (uint64_t j = 0; j < records_count; j++) {
            const uint8_t* record_ptr = residuals_mapped_u8 + j * record_size;
            const uint8_t* chunk_ptr = record_ptr + pq_index * pq_centroids_record_size;

            //const float* data = reinterpret_cast<const float*>(chunk_ptr);
            //std::cerr << j << ") " << data[0] << ", " << data[1] << ", " << data[2] << ", " << data[3] << std::endl;
            pq_builder.set_record(j, chunk_ptr);
        }
        //std::cerr << std::endl;

        ret = pq_builder.init_centroids_kmeans_plus_plus();
        CHECK(ret)

        for (uint64_t n = 0; n < 8; n++) {
            ret = pq_builder.recalc_centroids();
            CHECK(ret)
        }

        const std::string pq_centroids_path = index_path + "/pq_centroids_" + std::to_string(pq_index);
        ret = Centroids::write_centroids(pq_centroids_path, pq_builder);
        CHECK(ret)

        return 0;
    }

private:
    const DatasetType type;
    const uint64_t record_size;
    const uint64_t pq_centroids_record_size;
    const uint64_t pq_centroid_dim;
    const uint64_t pq_centroids_count;
    const uint64_t records_count;
    const uint8_t* residuals_mapped_u8;
    const std::string& index_path;

};

Ret Dataset::make_pq_centroids(uint64_t chunk_count, uint64_t pq_centroids_count, ThreadPool* thread_pool, MakePqCentroidsTestFunc test_func) {
    READ_OP_HEADER

    if (metadata_.dim % chunk_count != 0) {
        return "DIMENSION is not divisible by the number of PQ centroids";
    }

    if (!centroids_) {
        return "Centroids not initialized";
    };

    const std::string index_path = path_ + "/index_" + std::to_string(metadata_.index_id);

    //******************************************************************************************************* */
    const std::string residuals_path = index_path + "/residuals";
    if (!std::filesystem::exists(residuals_path)) {
        return "Residuals file does not exist";
    }

    int residuals_fd = open(residuals_path.c_str(), O_RDONLY);
    if (residuals_fd == -1) {
        return std::format("Failed to open residuals file at '{}'", residuals_path);
    }

    struct stat st;
    if (fstat(residuals_fd, &st) == -1) {
        close(residuals_fd);
        return std::format("Failed to stat residuals file at '{}'", residuals_path);
    }

    void* residuals_mapped = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, residuals_fd, 0);
    close(residuals_fd);
    if (residuals_mapped == MAP_FAILED) {
        return std::format("Failed to mmap residuals file at '{}'", residuals_path);
    }

    const std::experimental::scope_exit unmapper([&] {
        munmap(residuals_mapped, st.st_size);
    });

    uint8_t* residuals_mapped_u8 = reinterpret_cast<uint8_t*>(residuals_mapped);
    const uint64_t record_size = metadata_.record_size();
    const uint64_t records_count = st.st_size / record_size;

    const uint64_t pq_centroids_record_size = metadata_.record_size() / chunk_count;
    const uint64_t pq_centroid_dim = metadata_.dim / chunk_count;

    //******************************************************************************************************* */
    (void)thread_pool;
    const PQCentroidWorker worker(
        metadata_.type,
        record_size,
        pq_centroids_record_size,
        pq_centroid_dim,
        pq_centroids_count,
        records_count,
        residuals_mapped_u8,
        index_path);

    Ret res{0};
    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(chunk_count);

        for (uint64_t i = 0; i < chunk_count; i++) {
            futures.push_back(thread_pool->submit([&worker, i] {
                return worker.build_pq_centroids(i);
            }));
        }

        for (size_t i = 0; i < chunk_count; i++) {
            auto ret = futures[i].get();
            if (ret != 0) {
                res = ret;
            }
        }

    } else {
        for (uint64_t i = 0; i < chunk_count; i++) {
            //std::cerr << "Calculating PQ centroids for chunk " << i << " ..." << std::endl;
            auto ret = worker.build_pq_centroids(i);
            CHECK(ret)
        }
    }

    CHECK(res)

    metadata_.pq_count = chunk_count;
    auto ret = write_metadata();
    CHECK(ret)

    ret = load_pq_centroids();
    CHECK(ret)

    if (test_func) {
        (void)test_func(pq_centroids_);
    }

    return 0;
}

Ret Dataset::load_pq_centroids() {
    if (metadata_.pq_count == 0) {
        return 0;
    }

    std::string index_path = path_ + "/index_" + std::to_string(metadata_.index_id);

    pq_centroids_.resize(metadata_.pq_count);

    for (size_t pq_index = 0; pq_index < metadata_.pq_count; pq_index++) {
        const std::string pq_centroids_path = index_path + "/pq_centroids_" + std::to_string(pq_index);
        pq_centroids_[pq_index] = std::make_unique<Centroids>();
        auto ret = pq_centroids_[pq_index]->init(pq_centroids_path);
        CHECK(ret)
    }

    return 0;
}

Ret Dataset::mock_ivf(uint64_t centroids_count, uint64_t sample_count, MockIvfTestFunc test_func) {
    READ_OP_HEADER

    const uint64_t prev_index_id = metadata_.index_id;

    IvfBuilder builder(metadata_.type, metadata_.dim, centroids_count, sample_count);
    CHECK(builder.init())
    CHECK(init_centroids_kmeans_plus_plus(builder, nullptr))
    for (uint64_t i = 0; i < 8; i++) {
        CHECK(builder.recalc_centroids())
    }
    CHECK(write_index(builder))

    assert(prev_index_id + 1 == metadata_.index_id);

    if (test_func) {
        test_func(centroids_);
    }

    /*
    for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
        auto node = get_node(node_index);
        if (!node) {
            return -1;
        }

        CHECK(node->mock_ivf(builder, metadata_.index_id))
        nodes_[node_index].reset();
    }

    CHECK(make_residual(residuals_count))

    const uint64_t chunk_count = 4;
    CHECK(make_pq_centroids(chunk_count))
    */

    return 0;
}

} // namespace sketch
