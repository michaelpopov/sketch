#pragma once
#include "dataset_node.h"
#include "centroids.h"
#include "rw_lock.h"
#include "shared_types.h"
#include <atomic>
#include <functional>
#include <memory>
#include <queue>
#include <vector>

namespace sketch {

class IvfBuilder;
class ThreadPool;

using MakeResidualsTestFunc = std::function<Ret(DatasetType type, uint64_t dim, uint64_t count, const uint8_t* data)>;
using MakePqCentroidsTestFunc = std::function<Ret(const std::vector<std::unique_ptr<Centroids>>& pq_centroids)>;
using MockIvfTestFunc = std::function<Ret(const std::unique_ptr<Centroids>& centroids)>;

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
    Ret dump_ivf();
    Ret make_residuals(uint64_t count, ThreadPool* thread_pool = nullptr);
    Ret make_pq_centroids(uint64_t chunk_count, uint64_t pq_centroids_depth = 256, ThreadPool* thread_pool = nullptr);
    Ret mock_ivf(uint64_t centroids_count, uint64_t sample_count, uint64_t chunk_count, uint64_t pq_centroids_depth=256);
    Ret write_pq_vectors(ThreadPool* thread_pool = nullptr);

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
    std::vector<std::unique_ptr<Centroids>> pq_centroids_;
    RWLock rw_lock_;

private:
    Ret write_metadata();
    Ret read_metadata();
    DatasetNodePtr get_node(uint64_t tag);

    Ret write_centroids(IvfBuilder& builder);
    Ret write_index_internal(ThreadPool* thread_pool = nullptr);
    Ret update_and_write_metadata();
    Ret load_pq_centroids();


public:
    void set_make_residuals_test_func(MakeResidualsTestFunc func) { make_residuals_test_func_ = func; }
    void set_make_pq_centroids_test_func(MakePqCentroidsTestFunc func) { make_pq_centroids_test_func_ = func; }
    void set_mock_ivf_test_func(MockIvfTestFunc func) { mock_ivf_test_func_ = func; }

private:
    MakeResidualsTestFunc make_residuals_test_func_ = nullptr;
    MakePqCentroidsTestFunc make_pq_centroids_test_func_ = nullptr;
    MockIvfTestFunc mock_ivf_test_func_ = nullptr;
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

#define READ_OP_HEADER \
    if (shutting_down_) return -1; \
    const InUseMarker in_use_marker(in_use_count_); \
    const ReadGuard guard(rw_lock_);

#define WRITE_OP_HEADER \
    if (shutting_down_) return -1; \
    const InUseMarker in_use_marker(in_use_count_); \
    const WriteGuard guard(rw_lock_);


} // namespace sketch
