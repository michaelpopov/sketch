#include "dataset_node.h"
#include "centroids.h"
#include "ivf_builder.h"
#include "lmdb2.h"
#include "math.h"
#include "storage.h"
#include "string_utils.h"
#include "input_data.h"
#include "log.h"

#include <algorithm>
#include <experimental/scope>
#include <format>
#include <filesystem>
#include <random>

namespace sketch {

DatasetNode::DatasetNode(uint64_t id, const std::string& path)
  : id_(id),
    dir_path_(path + "/node_" + std::to_string(id)),
    path_(dir_path_ + "/data.bin")
{

}

DatasetNode::~DatasetNode() {
}

Ret DatasetNode::create(const DatasetMetadata& metadata, uint64_t initial_records_count) {
    type_ = metadata.type;
    dim_ = metadata.dim;

    if (std::filesystem::exists(dir_path_)) {
        return Ret(std::format("Dataset node directory {} already exists", dir_path_));
    }

    try {
        if (!std::filesystem::create_directories(dir_path_)) {
            return std::format("Failed to create dataset node directory {}", dir_path_);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return std::format("Filesystem error: {}", e.what());
    }

    std::string index_path = dir_path_ + "/index_0";
    if (!std::filesystem::create_directory(index_path)) {
        return std::format("Failed to create index directory {}", index_path);
    }

    auto ret = create_lmdb(index_path);
    if (ret != 0) {
        return ret;
    }

    record_size_ = metadata.record_size();
    storage_ = std::make_unique<Storage>(path_, record_size_);
    return storage_->create(initial_records_count);
}

Ret DatasetNode::create_lmdb(const std::string& path) {
    auto lmdb = std::make_unique<LmdbEnv>(path);
    int ret = lmdb->init();
    if (ret != 0) {
        return std::format("Failed to initialize LMDB: {}", ret);
    }

    ret = lmdb->create_db();
    if (ret != 0) {
        return std::format("Failed to create LMDB records table: {}", ret);
    }

    return 0;
}

std::unique_ptr<LmdbEnv> DatasetNode::open_lmdb(const std::string& path) {
    auto lmdb = std::make_unique<LmdbEnv>(path);
    int ret = lmdb->init();
    if (ret != 0) {
        return nullptr;
    }

    return lmdb;
}

Ret DatasetNode::init(const DatasetMetadata& metadata) {
    type_ = metadata.type;
    dim_ = metadata.dim;

    std::string index_path = dir_path_ + "/index_" + std::to_string(metadata.index_id);
    lmdb_ = open_lmdb(index_path);
    if (!lmdb_) {
        return "Failed to initialize LMDB";
    }

    record_size_ = metadata.record_size();
    storage_ = std::make_unique<Storage>(path_, record_size_);
    return storage_->init();
}

Ret DatasetNode::uninit() {
    if (storage_) {
        storage_->uninit();
        storage_.reset();
    }

    return 0;
}

Ret DatasetNode::prepare_load(const std::string& node_path, size_t nodes_count, LoadReport& report, const InputData& input_data) {
    auto lmdb_reader = lmdb_->open_db();
    if (!lmdb_reader) {
        return std::format("Failed to open LMDB records reader");
    }

    FILE* f = fopen(node_path.c_str(), "w");
    if (!f) {
        return std::format("Failed to open load file for node {} : {}", id_, node_path);
    }
    const std::experimental::scope_exit closer([&] {
        fclose(f);
    });

    uint64_t counter = 0;
    for (uint64_t index = 0; index < input_data.size(); index++) {
        auto item_opt = input_data.get(index);
        if (!item_opt.has_value()) {
            LOG_ERROR << "Mismatch input data size   index=" << index << "  size=" << input_data.size();
            break;
        }

        const auto& item = item_opt.value();
        uint64_t tag = 0;
        try {
            tag = u64_from_string_view(item.tag);
        } catch (const std::exception& e) {
            return "Failed to parse LOAD command data tag";
        }

        uint64_t matched_node_id = tag % nodes_count;
        if (matched_node_id != id_) {
            continue;
        }

        uint32_t record_id = INVALID_RECORD_ID;
        uint16_t cluster_id = InvalidClusterId;
        lmdb_reader->read_record(tag, record_id, cluster_id);

        fwrite(&counter, 1, sizeof(counter), f);
        fwrite(&tag, 1, sizeof(tag), f);
        fwrite(&record_id, 1, sizeof(record_id), f);
        fwrite(&cluster_id, 1, sizeof(cluster_id), f);
        fwrite(&index, 1, sizeof(index), f);

        counter++;
    }

    report.staged_count += counter;
    report.nodes_count++;

    return 0;
}

Ret DatasetNode::load(const std::string& node_path, const DatasetMetadata& metadata, 
            LoadReport& report, const InputData& input_data, Centroids* centroids) {
    auto records_writer = lmdb_->open_db(LmdbMode::Write);
    if (!records_writer) {
        return std::format("Failed to open LMDB records writer");
    }

    DataBuffer data_buffer(record_size_, HeaderSize);

    FILE* f = fopen(node_path.c_str(), "r");
    if (!f) {
        return std::format("Failed to open load file for node {} : {}", id_, node_path);
    }
    const std::experimental::scope_exit closer([&] {
        fclose(f);
    });

    uint64_t counter = 0;
    std::vector<char> item_data(16 * 1024);
    
    uint64_t n = 0;
    while (sizeof(counter) == fread(&counter, 1, sizeof(counter), f))  {
        report.staged_read_count++;

        uint64_t tag = 0;
        uint32_t record_id = 0;
        uint16_t cluster_id = 0;
        uint64_t index = 0;

        if (n++ != counter) {
            return std::format("Invalid format file {}", node_path);
        }
        if (sizeof(tag) != fread(&tag, 1, sizeof(tag), f))  {
            return std::format("Failed to read tag from file {}", node_path);
        }
        if (sizeof(record_id) != fread(&record_id, 1, sizeof(record_id), f))  {
            return std::format("Failed to read record_id from file {}", node_path);
        }
        if (sizeof(cluster_id) != fread(&cluster_id, 1, sizeof(cluster_id), f))  {
            return std::format("Failed to read tag from file {}", node_path);
        }
        if (sizeof(index) != fread(&index, 1, sizeof(index), f)) {
            return std::format("Failed to read item size from file {}", node_path);
        }

        auto item_opt = input_data.get(index);
        if (!item_opt.has_value()) {
            return std::format("Failed to get data item {}", index);
        }
        const auto& item = item_opt.value();

        bool is_empty = false;
        Ret cret{0};
        switch (metadata.type) {
            case DatasetType::f16: cret = convert_ptr_f16(item.data, data_buffer.record_ptr(), metadata.dim, is_empty); break;
            case DatasetType::f32: cret = convert_ptr_f32(item.data, data_buffer.record_ptr(), metadata.dim, is_empty); break;
            case DatasetType::u8: return "U8 type not supported"; break;
        }

        if (cret != 0) {
            report.conversion_errors_count++;
            return "Failed to convert vector line";
        }

        if (is_empty) {
            if (record_id == INVALID_RECORD_ID) {
                return "Invalid record_id for delete record operation";
            }
            auto ret = storage_->delete_record(record_id);
            if (ret != 0) {
                return ret;
            }
            auto iret = records_writer->delete_record(tag, record_id, cluster_id);
            if (iret != 0) {
                return "Failed to delete record in LMDB";
            }

            report.removed_count++;

        } else {
            data_buffer.set_header(tag);
            if (record_id != INVALID_RECORD_ID) {
                auto ret = storage_->update_record(record_id, data_buffer);
                if (ret != 0) {
                    return ret;
                }
                auto iret = records_writer->delete_index(cluster_id, record_id);
                if (iret != 0) {
                    return "Failed to update index in LMDB";
                }

                report.updated_count++;

            } else {
                auto [ local_record_id, ret ] = storage_->put_record(data_buffer);
                if (ret != 0) {
                    return ret;
                }

                record_id = local_record_id;
                report.added_count++;
            }

            uint16_t cluster_id = InvalidClusterId;
            if (centroids != nullptr) {
                cluster_id = centroids->find_nearest_centroid(data_buffer.record_ptr(), type_, dim_);
            }

            int iret = records_writer->write_record(tag, record_id, cluster_id);
            if (iret != 0) {
                return std::format("Failed to write to LMDB: {}", iret);
            }
        }

        report.processed_count++;
    }

    int iret = records_writer->commit();
    if (iret != 0) {
        return std::format("Failed to commit to LMDB: {}", iret);
    }

    if (!feof(f)) {
        return std::format("Failed to read file {}", node_path);
    }

    return 0;
}

Ret DatasetNode::dump(const std::string& dump_path, const DatasetMetadata& metadata) {
    auto records_reader = lmdb_->open_db();
    if (!records_reader) {
        return std::format("Failed to open LMDB records reader");
    }

    FILE* f = stdout;

    if (!dump_path.empty()) {
        const std::string node_path = dump_path + "/dump_node_" + std::to_string(id_);
        FILE* f = fopen(node_path.c_str(), "w");
        if (!f) {
            return std::format("Failed to open load file for node {} : {}", id_, node_path);
        }
    }
    const std::experimental::scope_exit closer([&] {
        if (f != stdout) {
            fclose(f);
        }
    });

    for (uint64_t index = 0; ; index++) {
        Record record;
        auto ret = storage_->scan_record(index, record);
        if (ret == ScanResult::Finished) {
            break;
        }

        if (ret == ScanResult::Deleted) {
            continue;
        }

        uint32_t record_id = 0;
        uint16_t cluster_id = 0;
        int iret = records_reader->read_record(record.tag, record_id, cluster_id);
        if (iret != 0) {
            return std::format("Failed to read from LMDB: {}   tag={}", iret, record.tag);
        }

        if (index != record_id) {
            return "Invalid record_id in LMDB";
        }

        fprintf(f, "%lu : [ ", record.tag);
        switch (metadata.type) {
            case DatasetType::f32: {
                float* data = (float*)record.data;
                for (uint64_t i = 0; i < metadata.dim; i++) {
                    fprintf(f, "%f, ", data[i]);
                }
                break;
            }
            case DatasetType::f16: {
                float16_t* data = (float16_t*)record.data;
                for (uint64_t i = 0; i < metadata.dim; i++) {
                    fprintf(f, "%f, ", data[i]);
                }
                break;
            }
            case DatasetType::u8: {
                return "U8 type not supported";
            }
        }
        fprintf(f, " ]\n");
    }

    return 0;
}

Ret DatasetNode::find_tag(uint64_t tag) {
    for (uint64_t index = 0; ; index++) {
        Record record;
        auto ret = storage_->scan_record(index, record);
        if (ret == ScanResult::Finished) {
            break;
        }

        if (ret == ScanResult::Deleted) {
            continue;
        }

        if (record.tag == tag) {
            return Ret(0, std::format("Tag {} found", tag));
        }
    }

    return Ret(-1, std::format("Tag {} not found", tag));
}

Ret DatasetNode::find_data(const std::vector<uint8_t>& data) {
    assert(data.size() <= record_size_);

    for (uint64_t index = 0; ;) {
        Record record;
        auto ret = storage_->scan_record(index, record);
        if (ret == ScanResult::Finished) {
            break;
        }

        if (ret == ScanResult::Deleted) {
            index++;
            continue;
        }

        if (memcmp(record.data, data.data(), data.size()) == 0) {
            return Ret(0, std::format("{}", record.tag));
        }

        index++;
    }

    return Ret(-1, "Data not found");
}

template <typename T>
double calc_dist(KnnType type, const T* a, const T* b, uint64_t dim) {
    switch (type) {
        case KnnType::L1: return distance_L1(a, b, dim);
        case KnnType::L2: return distance_L2(a, b, dim);
        case KnnType::COS: return distance_cos(a, b, dim);
        default: return 0.0;
    };
    return 0.0;
}

DistItems DatasetNode::knn(const DatasetMetadata& metadata, KnnType type, uint64_t count, const std::vector<uint8_t>& data, uint64_t skip_tag) {
    std::priority_queue<DistItem> pq;

    for (uint64_t index = 0; ; index++) {
        Record record;
        auto ret = storage_->scan_record(index, record);
        if (ret == ScanResult::Finished) {
            break;
        }

        if (ret == ScanResult::Deleted) {
            continue;
        }

        if (record.tag == skip_tag) {
            continue;
        }

        double dist = 0.0;
        switch (metadata.type) {
            case DatasetType::f32:
                dist = calc_dist(type, (float*)record.data, (float*)data.data(), metadata.dim);
                break;
            case DatasetType::f16:
                dist = calc_dist(type, (float16_t*)record.data, (float16_t*)data.data(), metadata.dim);
                break;
            case DatasetType::u8:
                dist = 0.0;
                break;
        }

        pq.push(DistItem{ .dist=dist, .record_id=index, .tag=record.tag});
        if (pq.size() > count) {
            pq.pop();
        }
    }

    DistItems res;
    while (!pq.empty()) {
        res.push_back(pq.top());
        pq.pop();
    }

    return res;
}

Ret DatasetNode::read_record_id(const uint64_t tag, uint32_t& out_id) {
    auto records_reader = lmdb_->open_db();    
    if (!records_reader) {
        return std::format("Failed to open LMDB records reader");
    }

    uint16_t cluster_id = InvalidClusterId;
    int iret = records_reader->read_record(tag, out_id, cluster_id);
    if (iret != 0) {
        return std::format("Failed to read from LMDB: {}", iret);
    }

    return 0;
}

DistItems  DatasetNode::ann(const std::vector<uint16_t>& cluster_ids, uint64_t count,
    const std::vector<uint8_t>& data, uint64_t skip_tag) {
    
    DistItems res;
    std::priority_queue<DistItem> pq;

    auto cursor_reader = lmdb_->open_db();
    if (!cursor_reader) {
        return res;
    }

    for (const auto cluster_id : cluster_ids) {
        int ret = cursor_reader->open_cursor(cluster_id);
        const std::experimental::scope_exit closer([&] {
            cursor_reader->close_cursor();
        });
        if (ret != 0) {
            LOG_TRACE << "Failed to open cursor for cluster_id=" << cluster_id;
            continue;
        }

        uint32_t scanned_count = 0;
        uint32_t record_id = 0;
        while (0 == cursor_reader->next(record_id)) {
            Record record;
            auto ret = storage_->scan_record(record_id, record);
            if (ret != ScanResult::Ok) {
                continue;
            }

            if (record.tag == skip_tag) {
                continue;
            }

            scanned_count++;

            double dist = 0.0;
            switch (type_) {
                case DatasetType::f32:
                    dist = calc_dist(KnnType::L2, (float*)record.data, (float*)data.data(), dim_);
                    break;
                case DatasetType::f16:
                    dist = calc_dist(KnnType::L2, (float16_t*)record.data, (float16_t*)data.data(), dim_);
                    break;
                case DatasetType::u8:
                    dist = 0.0;
                    break;
            }

            pq.push(DistItem{ .dist=dist, .record_id=record_id, .tag=record.tag});
            if (pq.size() > count) {
                pq.pop();
            }
        }
    }

    while (!pq.empty()) {
        res.push_back(pq.top());
        pq.pop();
    }

    return res;
}

Ret DatasetNode::gc(uint64_t current_index_id) {
    for (size_t i = 0; i + 1 < current_index_id; i++) {
        const std::string index_path = dir_path_ + "/index_" + std::to_string(i);
        if (std::filesystem::exists(index_path)) {
            std::filesystem::remove_all(index_path);
        }
    }

    return 0;
}

} // namespace sketch
