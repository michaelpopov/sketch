#include "engine.h"
#include "config.h"
#include "math.h"
#include "storage.h"
#include "string_utils.h"
#include "input_data.h"
#include "thread_pool.h"
#include "log.h"

#include <format>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <unordered_set>

#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

namespace sketch {

static Ret make_error(const std::string& message) {
    LOG_ERROR << message;
    return message;
}

/*********************************************************************
 *    Engine
 */
Engine::Engine(const Config& config)
  : config_(config)
{}

Engine::~Engine() {
    // no-op
}

Ret Engine::init() {
    std::filesystem::path data_path = config_.data_path;
   
    try {
        if (!std::filesystem::exists(data_path)) {
            if (std::filesystem::create_directory(data_path) != 0) {
                return make_error(std::format("Failed to create data directory '{}'", data_path.string()));
            }
        }

        for (const auto& entry : std::filesystem::directory_iterator(data_path)) {
            if (entry.is_directory()) {
                const std::string catalog_name = entry.path().filename().string();
                auto db = std::make_shared<Catalog>(config_, catalog_name);
                if (db->init() != 0) {
                    return make_error(std::format("Failed to initialize catalog '{}'", catalog_name));
                }
                catalogs_[catalog_name] = db;
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return make_error(std::format("Filesystem error while initializing engine: {}", e.what()));
    }

    return 0;
}

Ret Engine::create_catalog(const CmdCreateCatalog& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    if (catalogs_.find(catalog_name) != catalogs_.end()) {
        return std::format("Catalog '{}' already exists", cmd.catalog_name);
    }

    auto db = std::make_shared<Catalog>(config_, catalog_name);
    const auto ret = db->create();
    if (ret != 0) {
        return ret;
    }
  
    catalogs_[catalog_name] = db;

    return Ret(0, std::format("Successfully created catalog '{}'", cmd.catalog_name));
}

Ret Engine::drop_catalog(const CmdDropCatalog& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    auto catalog_iter = catalogs_.find(catalog_name);
    if (catalog_iter == catalogs_.end()) {
        return std::format("Catalog '{}' does not exist", cmd.catalog_name);
    }

    const auto ret = catalog_iter->second->remove();
    if (ret != 0) {
        return ret;
    }

    catalogs_.erase(catalog_iter);
  
    return Ret(0, std::format("Successfully dropped catalog '{}'", cmd.catalog_name));
}

Ret Engine::list_catalogs(const CmdListCatalogs& cmd) {
    (void)cmd;

    std::vector<std::string> catalog_names;
    for (const auto& [catalog_name, _] : catalogs_) {
        catalog_names.push_back(catalog_name);
    }

    std::sort(catalog_names.begin(), catalog_names.end());

    std::string result;
    for (const auto& catalog_name : catalog_names) {
        result += catalog_name + "\n";
    }

    return Ret(0, result, true);
}

Ret Engine::create_dataset(const CmdCreateDataset& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    auto catalog_iter = catalogs_.find(catalog_name);
    if (catalog_iter == catalogs_.end()) {
        return std::format("Catalog '{}' does not exist", cmd.catalog_name);
    }

    DatasetMetadata metadata {
        .type = cmd.type,
        .dim = cmd.dim,
        .nodes_count = cmd.nodes_count
    };
    return catalog_iter->second->create_dataset(cmd.dataset_name, metadata);
}

Ret Engine::drop_dataset(const CmdDropDataset& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    auto catalog_iter = catalogs_.find(catalog_name);
    if (catalog_iter == catalogs_.end()) {
        return std::format("Catalog '{}' does not exist", cmd.catalog_name);
    }

    return catalog_iter->second->drop_dataset(cmd.dataset_name);
}

Ret Engine::list_datasets(const CmdListDatasets& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    auto catalog_iter = catalogs_.find(catalog_name);
    if (catalog_iter == catalogs_.end()) {
        return std::format("Catalog '{}' does not exist", cmd.catalog_name);
    }

    return catalog_iter->second->list_datasets();
}

Ret Engine::show_dataset(const CmdShowDataset& cmd) {
    const std::string catalog_name(cmd.catalog_name);
    auto catalog_iter = catalogs_.find(catalog_name);
    if (catalog_iter == catalogs_.end()) {
        return std::format("Catalog '{}' does not exist", cmd.catalog_name);
    }

    auto dataset = catalog_iter->second->find_dataset(cmd.dataset_name);
    if (!dataset) {
        return std::format("Dataset '{}' does not exist in catalog '{}'", cmd.dataset_name, cmd.catalog_name);
    }

    const DatasetMetadata& metadata = dataset->metadata();

    std::string result;
    switch (metadata.type) {
        case DatasetType::f32: result += std::format("Type: {}\n", "f32"); break;
        case DatasetType::f16: result += std::format("Type: {}\n", "f16"); break;
    }
    result += std::format("Dim: {}\n", metadata.dim);
    result += std::format("Nodes: {}\n", metadata.nodes_count);

    return Ret(0, result, true);
}

DatasetPtr Engine::find_dataset(const std::string_view& catalog_name, const std::string_view& dataset_name) {
    auto catalog_iter = catalogs_.find(std::string(catalog_name));
    if (catalog_iter == catalogs_.end()) {
        return nullptr;
    }

    return catalog_iter->second->find_dataset(dataset_name);
}

void Engine::start_tread_pool(size_t num_threads) {
    if (num_threads == 0) {
        num_threads = std::thread::hardware_concurrency();
        if (num_threads == 0) num_threads = 4;
    }
    thread_pool_ = std::make_unique<ThreadPool>(num_threads);
}

ThreadPool* Engine::thread_pool() {
    return thread_pool_.get();
}

} // namespace sketch
