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

Ret DatasetNode::sample_records(IvfBuilder& builder, uint32_t from, uint32_t count) {
    assert(storage_);

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, storage_->records_count()-1);

    uint32_t index = from;

    // Allow certain number of records to be skipped if they are deleted.
    uint32_t skip_count = count / 10;
    for (uint32_t i = 0; i < count; ) {
        if (index >= storage_->records_count()) {
            break;
        }

        auto record_id = dis(gen);
        if (storage_->is_deleted(record_id)) {
            if (skip_count > 0) {
                skip_count--;
                continue;
            }
        }

        builder.set_record(index, storage_->get_record_data(record_id));
        index++;
        i++;
    }

    return 0;
}

Ret DatasetNode::write_index(const Centroids& centroids, uint64_t index_id) {
    const std::string index_path = dir_path_ + "/index_" + std::to_string(index_id);
    if (!std::filesystem::exists(index_path)) {
        std::filesystem::create_directory(index_path);
    }

    auto ret = create_lmdb(index_path);
    if (ret != 0) {
        return ret;
    }

    auto lmdb = open_lmdb(index_path);
    if (!lmdb) {
        return "Failed to initialize LMDB";
    }

    auto records_writer = lmdb->open_db(LmdbMode::Write);
    if (!records_writer) {
        return std::format("Failed to open LMDB records writer");
    }

    for (uint64_t record_id = 0; ; record_id++) {
        Record record;
        auto scan_ret = storage_->scan_record(record_id, record);
        if (scan_ret == ScanResult::Finished) {
            break;
        }

        if (scan_ret == ScanResult::Deleted) {
            continue;
        }

        uint16_t cluster_id = centroids.find_nearest_centroid(record.data, type_, dim_);
        auto ret = records_writer->write_record(record.tag, record_id, cluster_id);
        if (ret != 0) {
            return ret;
        }
    }

    return records_writer->commit();
}

Ret DatasetNode::make_residuals(const Centroids& centroids, uint8_t* mapped_u8, uint64_t count) {
    auto cursor_reader = lmdb_->open_db();
    if (!cursor_reader) {
        return "Failed to open LMDB records reader";
    }

    std::random_device rd;
    std::mt19937 gen(rd());

    uint64_t node_offset = id_ * count * record_size_;
    uint8_t* node_ptr = mapped_u8 + node_offset;

    uint64_t per_cluster_count = count / centroids.size();
    if (count != per_cluster_count * centroids.size()) {
        per_cluster_count++;
    }

    std::vector<uint32_t> record_ids(per_cluster_count);

    uint32_t processed_count = 0;
    for (uint32_t cluster_id = 0; cluster_id < centroids.size(); cluster_id++) {
        int iret = cursor_reader->open_cursor(cluster_id);
        if (iret != 0) {
            LOG_TRACE << "Failed to open cursor for cluster_id=" << cluster_id;
            continue;
        }
        const std::experimental::scope_exit closer([&] {
            cursor_reader->close_cursor();
        });

        uint32_t scanned_count = 0;
        uint32_t record_id = 0;
        while (0 == cursor_reader->next(record_id)) {
            Record record;
            auto ret = storage_->scan_record(record_id, record);
            if (ret != ScanResult::Ok) {
                continue;
            }

            if (scanned_count < per_cluster_count) {
                record_ids[scanned_count] = record_id;
            } else {
                std::uniform_int_distribution<> distrib(0, scanned_count - 1);
                uint32_t j = distrib(gen);
                if (j < per_cluster_count) {
                    record_ids[j] = record_id;
                }
            }

            scanned_count++;
        }

        uint64_t cluster_offset = cluster_id * per_cluster_count * record_size_;
        uint8_t* cluster_ptr = node_ptr + cluster_offset;
        const uint8_t* centroid = centroids.get_centroid(cluster_id);

        for (uint64_t j = 0; j < per_cluster_count && processed_count < count; j++, processed_count++) {
            Record record;
            storage_->scan_record(record_ids[j], record);

            uint8_t* residual_data_ptr = cluster_ptr + j * record_size_;

            // Calculate residual
            switch (type_) {
                case DatasetType::f32:
                    calc_residual((float*)record.data, (float*)centroid, (float*)residual_data_ptr, dim_);
                    break;
                case DatasetType::f16: {
                    calc_residual((float16_t*)record.data, (float16_t*)centroid, (float16_t*)residual_data_ptr, dim_);
                    break;
                }
            }
        }
    }

    return 0;
}

} // namespace sketch
