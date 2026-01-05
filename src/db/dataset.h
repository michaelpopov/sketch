#pragma once
#include "dataset_node.h"
#include "centroids.h"
#include "rw_lock.h"
#include "shared_types.h"
#include <atomic>
#include <memory>
#include <queue>
#include <vector>

namespace sketch {

class IvfBuilder;
class ThreadPool;

class Dataset {
    friend class DatasetHolder;
public:
    Dataset(const std::string& name, const std::string& path) : name_(name), path_(path) {}

    Ret create(const DatasetMetadata& metadata);
    Ret remove();
    
    Ret init();
    Ret uninit();

    const DatasetMetadata& metadata() const { return metadata_; }

    Ret load(const std::string_view& input_path, LoadReport& report, ThreadPool* thread_pool = nullptr);
    Ret dump(const std::string_view& output_path, ThreadPool* thread_pool = nullptr);

    Ret find_tag(uint64_t tag, ThreadPool* thread_pool = nullptr);
    Ret find_data(const std::vector<uint8_t>& data, ThreadPool* thread_pool = nullptr);
    Ret knn(KnnType type, uint64_t count, const std::vector<uint8_t>& data, uint64_t skip_tag, ThreadPool* thread_pool = nullptr);

    Ret sample_records(IvfBuilder& builder, ThreadPool* thread_pool = nullptr);
    Ret init_centroids_kmeans_plus_plus(IvfBuilder& builder, ThreadPool* thread_pool = nullptr);
    Ret write_index(IvfBuilder& builder, ThreadPool* thread_pool = nullptr);
    Ret ann(uint64_t count, uint64_t nprobes, const std::vector<uint8_t>& data, uint64_t skip_tag, ThreadPool* thread_pool = nullptr);
    Ret gc();
    Ret show_ivf();

private:
    struct InUseMarker {
    public:
        InUseMarker(std::atomic<uint64_t>& in_use_count) : in_use_count_(in_use_count) {}
        ~InUseMarker() { in_use_count_.fetch_sub(1);  }
    private:
        std::atomic<uint64_t>& in_use_count_;
    };
private:
    const std::string name_;
    const std::string path_;
    DatasetMetadata metadata_;
    std::vector<DatasetNodePtr> nodes_;
    std::atomic<uint64_t> in_use_count_{0};
    std::atomic<bool> shutting_down_{false};
    std::unique_ptr<Centroids> centroids_;
    RWLock rw_lock_;

private:
    Ret write_metadata();
    Ret read_metadata();
    DatasetNodePtr get_node(uint64_t tag);

    Ret write_centroids(IvfBuilder& builder);
    Ret write_index_internal(ThreadPool* thread_pool = nullptr);
    Ret update_and_write_metadata();

};
using DatasetPtr = std::shared_ptr<Dataset>;
using Datasets = std::unordered_map<std::string, DatasetPtr>;

class DatasetHolder {
public:
    DatasetHolder(Dataset& dataset) : dataset_(dataset) {
        guard_ = std::make_unique<ReadGuard>(dataset_.rw_lock_);
        marker_ = std::make_unique<Dataset::InUseMarker>(dataset_.in_use_count_);
    }

    ~DatasetHolder() = default;

    bool is_shutting_down() const {
        return dataset_.shutting_down_.load();
    }
private:
    Dataset& dataset_;
    std::unique_ptr<ReadGuard> guard_;
    std::unique_ptr<Dataset::InUseMarker> marker_;
};

} // namespace sketch
