#pragma once
#include "shared_types.h"
#include <cstdint>
#include <memory>
#include <string>

namespace sketch {

class Engine;
class Dataset;

using DatasetPtr = std::shared_ptr<Dataset>;

struct CmdUseDataset {
    std::string_view catalog_name;
    std::string_view dataset_name;
};

class DataCommandProcessor {
public:
    DataCommandProcessor(Engine& engine);
    ~DataCommandProcessor();

    Ret process_command(Commands& commands, bool is_help);
    const CommandNames& get_supported_commands() const;

    // For testing purposes
    DatasetPtr current_dataset() const { return current_dataset_; }

private:
    Engine& engine_;
    DatasetPtr current_dataset_;

private:
    Ret process_use_cmd(Commands& commands, bool is_help);
    Ret process_generate_cmd(Commands& commands, bool is_help);
    Ret process_load_cmd(Commands& commands, bool is_help);
    Ret process_dump_cmd(Commands& commands, bool is_help);
    Ret process_find_cmd(Commands& commands, bool is_help);
    Ret process_knn_cmd(Commands& commands, bool is_help);
    Ret process_sample_cmd(Commands& commands, bool is_help);
    Ret process_kmeanspp_cmd(Commands& commands, bool is_help);
    Ret process_make_centroids_cmd(Commands& commands, bool is_help);
    Ret process_make_ivf_cmd(Commands& commands, bool is_help);
    Ret process_dump_ivf_cmd(Commands& commands, bool is_help);
    Ret process_ann_cmd(Commands& commands, bool is_help);
    Ret process_gc_cmd(Commands& commands, bool is_help);
    Ret process_make_residual_cmd(Commands& commands, bool is_help);
    Ret process_make_pq_centroids_cmd(Commands& commands, bool is_help);
    Ret process_mock_ivf_centroids_cmd(Commands& commands, bool is_help);

};

} // namespace sketch
