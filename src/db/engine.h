#pragma once
#include "config.h"
#include "ddl_command_processor.h"
#include "data_command_processor.h"
#include "dataset_node.h"
#include "dataset.h"
#include "catalog.h"
#include "shared_types.h"
#include <atomic>
#include <memory>
#include <queue>
#include <vector>
#include <unordered_map>

namespace sketch {

class Engine {
public:
    Engine(const Config& config);
    ~Engine();
    Ret init();

    Ret create_catalog(const CmdCreateCatalog& cmd);
    Ret drop_catalog(const CmdDropCatalog& cmd);
    Ret list_catalogs(const CmdListCatalogs& cmd);

    Ret create_dataset(const CmdCreateDataset& cmd);
    Ret drop_dataset(const CmdDropDataset& cmd);
    Ret list_datasets(const CmdListDatasets& cmd);
    Ret show_dataset(const CmdShowDataset& cmd);

    DatasetPtr find_dataset(const std::string_view& catalog_name, const std::string_view& dataset_name);

    void start_tread_pool(size_t num_threads = 0);
    ThreadPool* thread_pool();

private:
    const Config& config_;
    Catalogs catalogs_;
    std::unique_ptr<ThreadPool> thread_pool_;

};

} // namespace sketch