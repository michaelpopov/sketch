#pragma once
#include "config.h"
#include "ddl_command_processor.h"
#include "data_command_processor.h"
#include "shared_types.h"
#include <atomic>
#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>

namespace sketch {

class Centroids;
class IvfBuilder;
class Storage;
class InputData;
class ResultCollector;
class ThreadPool;
class LmdbEnv;
struct Record;
class InputData;

struct LoadReport {
    std::atomic<uint64_t> input_count{0};
    std::atomic<uint64_t> staged_count{0};
    std::atomic<uint64_t> staged_read_count{0};
    std::atomic<uint64_t> added_count{0};
    std::atomic<uint64_t> removed_count{0};
    std::atomic<uint64_t> updated_count{0};
    std::atomic<uint64_t> nodes_count{0};
    std::atomic<uint64_t> conversion_errors_count{0};
    std::atomic<uint64_t> processed_count{0};
};

using DistItems = std::vector<DistItem>;

using FindClusterIdResult = std::pair<uint16_t, Ret>;

class DatasetNode {
public:
    DatasetNode(uint64_t id, const std::string& path);
    ~DatasetNode();
    Ret create(const DatasetMetadata& metadata, uint64_t initial_records_count);
    Ret init(const DatasetMetadata& metadata);
    Ret uninit();

    Ret prepare_load(const std::string& node_path, size_t nodes_count, LoadReport& report, const InputData& input_data);
    Ret load(const std::string& node_path, const DatasetMetadata& metadata, 
                LoadReport& report, const InputData& input_data, Centroids* centroids);
    Ret dump(const std::string& dump_path, const DatasetMetadata& metadata);

    Ret find_tag(uint64_t tag);
    Ret find_data(const std::vector<uint8_t>& data);

    DistItems knn(const DatasetMetadata& metadata, KnnType type, uint64_t count, const std::vector<uint8_t>& data, uint64_t skip_tag);

    Ret sample_records(IvfBuilder& builder, uint32_t from, uint32_t count);
    Ret write_index(const Centroids& centroids, uint64_t index_id);
    DistItems ann(const std::vector<uint16_t>& cluster_ids, uint64_t count, const std::vector<uint8_t>& data, uint64_t skip_tag);
    Ret gc(uint64_t current_index_id);
    Ret make_residuals(const Centroids& centroids, uint8_t* mapped_u8, uint64_t count);

private:
    static constexpr uint64_t INVALID_TAG = 0xFFFFFFFFFFFFFFFF;
    static constexpr uint32_t INVALID_RECORD_ID = 0xFFFFFFFF;

private:
    const uint64_t id_;
    const std::string dir_path_;
    const std::string path_;
    std::unique_ptr<Storage> storage_;
    std::unique_ptr<LmdbEnv> lmdb_;
    uint64_t record_size_ = 0;
    uint64_t markers_count_ = 0;
    DatasetType type_ = DatasetType::f32;
    uint64_t dim_ = 0;
    uint64_t index_id = 0;

private:
    Ret read_record_id(const uint64_t tag, uint32_t& out_id);
    Ret create_lmdb(const std::string& path);
    std::unique_ptr<LmdbEnv> open_lmdb(const std::string& path);

};
using DatasetNodePtr = std::shared_ptr<DatasetNode>;

} // namespace sketch