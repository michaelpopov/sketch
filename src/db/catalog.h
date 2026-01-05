#pragma once
#include "config.h"
#include "ddl_command_processor.h"
#include "data_command_processor.h"
#include "dataset_node.h"
#include "dataset.h"
#include "shared_types.h"
#include <atomic>
#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>

namespace sketch {

class Catalog {
public:
    Catalog(const Config& cfg, const std::string& name) : config_(cfg), name_(name) {}
    Ret create();
    Ret remove();

    Ret init();

    Ret create_dataset(const std::string_view& dataset_name, const DatasetMetadata& metadata);
    Ret drop_dataset(const std::string_view& dataset_name);
    Ret list_datasets();

    DatasetPtr find_dataset(const std::string_view& dataset_name);

private:
    const Config& config_;
    const std::string name_;
    Datasets datasets_;
};
using CatalogPtr = std::shared_ptr<Catalog>;
using Catalogs = std::unordered_map<std::string, CatalogPtr>;

} // namespace sketch