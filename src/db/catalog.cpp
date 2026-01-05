#include "catalog.h"
#include "log.h"

#include <algorithm>
#include <format>
#include <filesystem>

#include <assert.h>

namespace sketch {

static Ret make_error(const std::string& message) {
    LOG_ERROR << message;
    return message;
}

Ret Catalog::create() {
    std::filesystem::path db_path = std::filesystem::path(config_.data_path) / name_;

    try {
        if (std::filesystem::exists(db_path)) {
            return std::format("Path '{}' exists", db_path.string());
        }

        if (!std::filesystem::create_directory(db_path)) {
            return std::format("Failed to create directory '{}'", db_path.string());
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return std::format("Filesystem error: {}", e.what());
    }

    return 0;
}

Ret Catalog::remove() {
    std::filesystem::path db_path = std::filesystem::path(config_.data_path) / name_;

    try {
        if (std::filesystem::exists(db_path)) {
            std::filesystem::remove_all(db_path);
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return std::format("Filesystem error: {}", e.what());
    }

    return 0;
}

Ret Catalog::init() {
    std::filesystem::path db_path = std::filesystem::path(config_.data_path) / name_;

    try {
        if (!std::filesystem::exists(db_path) || !std::filesystem::is_directory(db_path)) {
            return make_error(std::format("Catalog path '{}' does not exist or is not a directory", db_path.string()));
        }

        for (const auto& entry : std::filesystem::directory_iterator(db_path)) {
            if (entry.is_directory()) {
                const std::string dataset_name = entry.path().filename().string();
                auto dataset = std::make_shared<Dataset>(dataset_name, entry.path().string());
                if (dataset->init() != 0) {
                    return make_error(std::format("Failed to initialize dataset '{}' in catalog '{}'", dataset_name, name_));
                }
                datasets_[dataset_name] = dataset;
            }
        }
    } catch (const std::filesystem::filesystem_error& e) {
        return make_error(std::format("Filesystem error while loading catalog '{}': {}", name_, e.what()));
    }

    return 0;
}

Ret Catalog::create_dataset(const std::string_view& dataset_name, const DatasetMetadata& metadata) {
    const std::string name(dataset_name);
    if (datasets_.find(name) != datasets_.end()) {
        return std::format("Dataset '{}' already exists in catalog '{}'", dataset_name, name_);
    }

    std::filesystem::path dataset_path = std::filesystem::path(config_.data_path) / name_ / dataset_name;

    auto dataset = std::make_shared<Dataset>(name, dataset_path.string());
    auto ret = dataset->create(metadata);
    if (ret != 0) {
        return ret;
    }
  
    dataset = std::make_shared<Dataset>(name, dataset_path.string());
    ret = dataset->init();
    if (ret != 0) {
        return ret;
    }

    datasets_[name] = dataset;

    return Ret(0, std::format("Successfully created dataset '{}'", dataset_name));
}

Ret Catalog::drop_dataset(const std::string_view& dataset_name) {
    auto dataset_iter = datasets_.find(std::string(dataset_name));
    if (dataset_iter == datasets_.end()) {
        return std::format("Dataset '{}' does not exist in catalog '{}'", dataset_name, name_);
    }

    const auto ret = dataset_iter->second->remove();
    if (ret != 0) {
        return ret;
    }
  
    datasets_.erase(dataset_iter);
  
    return Ret(0, std::format("Successfully dropped dataset '{}'", dataset_name));
}

Ret Catalog::list_datasets() {
    std::vector<std::string> dataset_names;
    dataset_names.reserve(datasets_.size());
    for (const auto& [dataset_name, _] : datasets_) {
        dataset_names.push_back(dataset_name);
    }
    std::sort(dataset_names.begin(), dataset_names.end());

    std::string result;
    for (const auto& dataset_name : dataset_names) {
        result += dataset_name + "\n";
    }

    return Ret(0, result, true);
}

DatasetPtr Catalog::find_dataset(const std::string_view& dataset_name) {
    auto it = datasets_.find(std::string(dataset_name));
    if (it != datasets_.end()) {
        return it->second;
    }
    return nullptr;
}

} // namespace sketch
