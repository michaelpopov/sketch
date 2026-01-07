#include "dataset.h"
#include "centroids.h"
#include "ivf_builder.h"
#include "math.h"
#include "string_utils.h"
#include "input_data.h"
#include "thread_pool.h"
#include "log.h"

#include <experimental/scope>
#include <filesystem>
#include <format>
#include <fstream>
#include <iostream>
#include <sstream>

#include <assert.h>

namespace sketch {

#define READ_OP_HEADER \
    if (shutting_down_) return -1; \
    const InUseMarker in_use_marker(in_use_count_); \
    const ReadGuard guard(rw_lock_);

#define WRITE_OP_HEADER \
    if (shutting_down_) return -1; \
    const InUseMarker in_use_marker(in_use_count_); \
    const WriteGuard guard(rw_lock_);

static Ret make_error(const std::string& message) {
    LOG_ERROR << message;
    return message;
}

Ret Dataset::create(const DatasetMetadata& metadata) {
    metadata_ = metadata;
    nodes_.resize(metadata_.nodes_count);

    try {
        if (std::filesystem::exists(path_)) {
            return std::format("Dataset directory '{}' exists already", path_);
        }

        if (!std::filesystem::create_directory(path_)) {
            return std::format("Failed to create dataset directory '{}'", path_);
        }

        write_metadata();

    } catch (const std::filesystem::filesystem_error& e) {
        return std::format("Filesystem error: {}", e.what());
    }

    // TODO: Make it configurable.
    const uint64_t initial_records_per_node = 64 * 1024 * 1024; // Initial 64M records per node

    for (size_t i = 0; i < nodes_.size(); i++) {
        auto& node = nodes_[i];
        node = std::make_shared<DatasetNode>(i, path_);
        int ret = node->create(metadata_, initial_records_per_node);
        if (ret != 0) {
            LOG_ERROR << std::format("Failed to create dataset node in dataset '{}'", path_);
            return ret;
        }
    }

    return 0;
}

Ret Dataset::remove() {
    try {
        if (!std::filesystem::exists(path_)) {
            return std::format("Dataset directory '{}' doesn't exist", path_);
        }
        
        std::filesystem::remove_all(path_);
    } catch (const std::filesystem::filesystem_error& e) {
        return std::format("Filesystem error: {}", e.what());
    }

    return 0;
}

Ret Dataset::init() {
    try {
        auto ret = read_metadata();
        if (ret != 0) {
            return ret;
        }

    } catch (const std::filesystem::filesystem_error& e) {
        return make_error(std::format("Filesystem error: {}", e.what()));
    } catch (const std::exception& e) {
        return make_error("Invalid value in metadata file");
    }

    nodes_.resize(metadata_.nodes_count);

    std::string index_path = path_ + "/index_" + std::to_string(metadata_.index_id);
    std::string centroids_path = index_path + "/centroids";
    if (std::filesystem::exists(centroids_path)) {
        centroids_ = std::make_unique<Centroids>();
        auto ret = centroids_->init(centroids_path);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

Ret Dataset::uninit() {
    shutting_down_ = true;
    for (size_t attempts=0; attempts < 100; attempts++) {
        if (in_use_count_ == 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    nodes_.clear();
    return 0;
}

Ret Dataset::write_metadata() {
    std::filesystem::path metadata_path = std::filesystem::path(path_) / "metadata";
    std::ofstream metadata_file(metadata_path);
    if (!metadata_file) {
        return std::format("Failed to create metadata file at '{}'", metadata_path.string());
    }

    metadata_file << "TYPE=";
    switch (metadata_.type) {
        case DatasetType::f32: metadata_file << "f32\n"; break;
        case DatasetType::f16: metadata_file << "f16\n"; break;
    }
    metadata_file << "DIMENSION=" << metadata_.dim << "\n";
    metadata_file << "NODES_COUNT=" << metadata_.nodes_count << "\n";
    metadata_file << "INDEX=" << metadata_.index_id << "\n";

    metadata_file.close();

    return 0;
}

Ret Dataset::read_metadata() {
    std::filesystem::path metadata_path = std::filesystem::path(path_) / "metadata";
    std::ifstream metadata_file(metadata_path);
    if (!metadata_file) {
        return make_error(std::format("Failed to open metadata file at '{}'", metadata_path.string()));
    }

    std::string line;
    while (std::getline(metadata_file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos == std::string::npos) {
            return make_error(std::format("Invalid line in metadata file '{}': {}", metadata_path.string(), line));
        }

        std::string key = line.substr(0, eq_pos);
        std::string value = line.substr(eq_pos + 1);

        if (key == "TYPE") {
            if (value == "f32") {
                metadata_.type = DatasetType::f32;
            } else if (value == "f16") {
                metadata_.type = DatasetType::f16;
            } else {
                return make_error(std::format("Unsupported TYPE value in metadata: '{}'", value));
            }
        } else if (key == "DIMENSION") {
            metadata_.dim = std::stoul(value);
        } else if (key == "NODES_COUNT") {
            metadata_.nodes_count = std::stoul(value);
        } else if (key == "INDEX") {
            metadata_.index_id = std::stoul(value);
        } else {
            return make_error(std::format("Unknown key in metadata file '{}': {}", metadata_path.string(), key));
        }
    }

    metadata_file.close();

    return 0;
}

DatasetNodePtr Dataset::get_node(size_t node_index) {
    assert(node_index < nodes_.size());

    if (!nodes_[node_index]) {
        nodes_[node_index] = std::make_shared<DatasetNode>(node_index, path_);
        int ret = nodes_[node_index]->init(metadata_);
        if (ret != 0) {
            nodes_[node_index] = nullptr;
            LOG_ERROR << std::format("Failed to initialize dataset node {}", node_index);
            return nullptr;
        }
    }

    return nodes_[node_index];
}

Ret Dataset::load(const std::string_view& input_path, LoadReport& report, ThreadPool* thread_pool) {
    WRITE_OP_HEADER

    std::string load_path = path_ + "/load";
    if (std::filesystem::exists(load_path)) {
        return std::format("Directory {} already exists", load_path);
    }

    if (!std::filesystem::create_directory(load_path)) {
        return std::format("Failed to create load directory {}", load_path);
    }

    const std::experimental::scope_exit closer([&] {
        std::filesystem::remove_all(load_path);
    });

    InputData input_data;
    int ret = input_data.init(input_path);
    if (ret != 0) {
        return "Failed to get test data from input file";
    }
    report.input_count = input_data.count();

    Ret result{0};
    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        /**************************************************************************
         *   Prepare temporary files with data pointers for each data node.
         */
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            std::string node_path = load_path + "/" + std::to_string(node_index);
            futures.push_back(thread_pool->submit([node_ptr = node.get(), node_path, nodes_count=nodes_.size(), &input_data, &report] {
                return node_ptr->prepare_load(node_path, nodes_count, report, input_data);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret ret = futures[node_index].get();
            if (ret != 0) {
                result = Ret(-1, std::format("Failed to prepare load for node {}: {}", node_index, ret.message()));
            }
        }

        futures.clear();

        /**************************************************************************
         *   Load data for each data node.
         */
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            std::string node_path = load_path + "/" + std::to_string(node_index);
            futures.push_back(thread_pool->submit([node_ptr = node.get(), node_path, md=metadata_, &input_data, &report, cents=centroids_.get()] {
                return node_ptr->load(node_path, md, report, input_data, cents);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret ret = futures[node_index].get();
            if (ret != 0) {
                result = Ret(-1, std::format("Failed to load for node {}: {}", node_index, ret.message()));
            }
        }
    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            std::string node_path = load_path + "/" + std::to_string(node_index);
            auto ret = node->prepare_load(node_path, nodes_.size(), report, input_data);
            if (ret != 0) {
                return std::format("Failed to prepare load for node {}: {}", node_index, ret.message());
            }
            ret = node->load(node_path, metadata_, report, input_data, centroids_.get());
            if (ret != 0) {
                return std::format("Failed to prepare load for node {}: {}", node_index, ret.message());
            }
        }
    }

    return result;
}

Ret Dataset::dump(const std::string_view& output_path, ThreadPool* thread_pool) {
    READ_OP_HEADER

    std::string dump_path;
    if (!output_path.empty()) {
        dump_path = std::format("{}/{}", output_path, name_);
        if (!std::filesystem::exists(dump_path)) {
            if (!std::filesystem::create_directories(dump_path)) {
                return std::format("Failed to create dump directory {}", dump_path);
            }
        }
    }

    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), dump_path, md=metadata_] {
                return node_ptr->dump(dump_path, md);
            }));
        }

        Ret result_ret{-1};
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret ret = futures[node_index].get();
            if (ret != 0) {
                result_ret = ret;
            }
        }

        return result_ret;

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto ret = node->dump(dump_path, metadata_);
            if (ret != 0) {
                return ret;
            }
        }
    }

    return 0;
}

Ret Dataset::find_tag(uint64_t tag, ThreadPool* thread_pool) {
    READ_OP_HEADER

    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), tag] {
                return node_ptr->find_tag(tag);
            }));
        }

        Ret result_ret{-1};
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret ret = futures[node_index].get();
            if (ret == 0) {
                if (result_ret != -1) {
                    LOG_ERROR << "Tag " << tag << " found in multiple nodes";
                }
                result_ret = ret;
            }
        }

        return result_ret;

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto ret = node->find_tag(tag);
            if (ret == 0) {
                return ret;
            }
        }
    }

    return Ret(-1, std::format("Tag {} not found", tag));
}

Ret Dataset::find_data(const std::vector<uint8_t>& data, ThreadPool* thread_pool) {
    READ_OP_HEADER

    if (thread_pool) {
        std::vector<std::future<Ret>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), &data] {
                return node_ptr->find_data(data);
            }));
        }

        Ret result_ret{-1, "Data not found"};
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            Ret ret = futures[node_index].get();
            if (ret == 0) {
                if (result_ret != -1) {
                    LOG_ERROR << "Data found in multiple nodes";
                }
                result_ret = ret;
            }
        }

        return result_ret;

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto ret = node->find_data(data);
            if (ret == 0) {
                return ret;
            }
        }
    }

    return "Data not found";
}

Ret Dataset::knn(KnnType type, uint64_t count, const std::vector<uint8_t>& data, uint64_t skip_tag, ThreadPool* thread_pool) {
    READ_OP_HEADER

    std::priority_queue<DistItem> pq;

    if (thread_pool) {
        std::vector<std::future<DistItems>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), md=metadata_, type, count, &data, skip_tag] {
                return node_ptr->knn(md, type, count, data, skip_tag);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto res = futures[node_index].get();
            for (auto& item : res) {
                pq.push(item);
                if (pq.size() > count) {
                    pq.pop();
                }
            }
        }

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto res = node->knn(metadata_, type, count, data, skip_tag);
            for (auto& item : res) {
                pq.push(item);
                if (pq.size() > count) {
                    pq.pop();
                }
            }
        }
    }

    std::vector<uint64_t> tags;
    while (!pq.empty()) {
        const auto item = pq.top();
        tags.push_back(item.tag);
        pq.pop();
    }

    std::sort( tags.begin(), tags.end());

    std::stringstream sstream;
    for (auto tag : tags) {
        sstream << tag << ", ";
    }

    return Ret(0, sstream.str());
}

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
    for (size_t i = 0; i < builder.centroids_count(); i++) {
        switch (metadata_.type) {
            case DatasetType::f32: {
                const float* f = reinterpret_cast<const float*>(builder.get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
            case DatasetType::f16: {
                const float16_t* f = reinterpret_cast<const float16_t*>(builder.get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
        }
    }

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
    for (size_t i = 0; i < centroids_->size() && i < 16; i++) {
        switch (metadata_.type) {
            case DatasetType::f32: {
                const float* f = reinterpret_cast<const float*>(centroids_->get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
            case DatasetType::f16: {
                const float16_t* f = reinterpret_cast<const float16_t*>(centroids_->get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
        }
    }

    return Ret(0, sstream.str(), true);
}

Ret Dataset::ann(uint64_t count, uint64_t nprobes, const std::vector<uint8_t>& data, uint64_t skip_tag, ThreadPool* thread_pool) {
    READ_OP_HEADER

    if (!centroids_) {
        return "Centroids not initialized";
    };

    std::vector<uint16_t> cluster_ids;
    centroids_->find_nearest_clusters(data.data(), metadata_.type, metadata_.dim, cluster_ids, nprobes);

    std::priority_queue<DistItem> pq;

    if (thread_pool) {
        std::vector<std::future<DistItems>> futures;
        futures.reserve(nodes_.size());

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            futures.push_back(thread_pool->submit([node_ptr = node.get(), &cluster_ids, count, &data, skip_tag] {
                return node_ptr->ann(cluster_ids, count, data, skip_tag);
            }));
        }

        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto res = futures[node_index].get();
            for (auto& item : res) {
                pq.push(item);
                if (pq.size() > count) {
                    pq.pop();
                }
            }
        }

    } else {
        for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
            auto node = get_node(node_index);
            if (!node) {
                return -1;
            }

            auto res = node->ann(cluster_ids, count, data, skip_tag);
            for (auto& item : res) {
                pq.push(item);
                if (pq.size() > count) {
                    pq.pop();
                }
            }
        }
    }

    std::vector<uint64_t> tags;
    while (!pq.empty()) {
        const auto item = pq.top();
        tags.push_back(item.tag);
        pq.pop();
    }

    std::sort( tags.begin(), tags.end());

    std::stringstream sstream;
    for (auto tag : tags) {
        sstream << tag << ", ";
    }

    return Ret(0, sstream.str());
}

Ret Dataset::gc() {
    WRITE_OP_HEADER

    for (size_t i = 0; i + 1 < metadata_.index_id; i++) {
        const std::string index_path = path_ + "/index_" + std::to_string(i);
        if (std::filesystem::exists(index_path)) {
            std::filesystem::remove_all(index_path);
        }
    }

    for (size_t node_index = 0; node_index < nodes_.size(); node_index++) {
        auto node = get_node(node_index);
        if (!node) {
            return -1;
        }

        auto ret = node->gc(metadata_.index_id);
        if (ret != 0) {
            return ret;
        }
    }

    return 0;
}

Ret Dataset::show_ivf() {
    READ_OP_HEADER

    if (!centroids_) {
        return "Centroids not initialized";
    };

    std::stringstream sstream;
    for (size_t i = 0; i < centroids_->size(); i++) {
        switch (metadata_.type) {
            case DatasetType::f32: {
                const float* f = reinterpret_cast<const float*>(centroids_->get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
            case DatasetType::f16: {
                const float16_t* f = reinterpret_cast<const float16_t*>(centroids_->get_centroid(i));
                for (size_t d = 0; d < metadata_.dim && d < 4; d++) {
                    sstream << f[d] << ", ";
                }
                sstream << "\n";
                break;
            }
        }
    }

    return Ret(0, sstream.str(), true);
}

} // namespace sketch