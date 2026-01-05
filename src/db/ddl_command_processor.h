#pragma once
#include "shared_types.h"
#include <string>

namespace sketch {

class Engine;

struct CmdCreateCatalog {
    std::string_view catalog_name;
};

struct CmdDropCatalog {
    std::string_view catalog_name;
};

struct CmdListCatalogs {
    // No parameters
};

struct CmdCreateDataset {
    std::string_view catalog_name;
    std::string_view dataset_name;
    DatasetType type;
    size_t dim;
    size_t nodes_count;
};

struct CmdDropDataset {
    std::string_view catalog_name;
    std::string_view dataset_name;
};

struct CmdListDatasets {
    std::string_view catalog_name;
};

struct CmdShowDataset {
    std::string_view catalog_name;
    std::string_view dataset_name;
};

class DDLCommandProcessor {
public:
    DDLCommandProcessor(Engine& engine) : engine_(engine) {}
    Ret process_command(Commands& commands, bool is_help);
    const CommandNames& get_supported_commands() const;

private:
    Engine& engine_;

private:
    Ret process_create_cmd(Commands& commands, bool is_help);
    Ret process_drop_cmd(Commands& commands, bool is_help);
    Ret process_list_cmd(Commands& commands, bool is_help);
    Ret process_show_cmd(Commands& commands, bool is_help);
    Ret process_dummy_cmd(Commands& commands, bool is_help);

    int get_properties_from_command(Commands& commands, size_t from_index, Properties& properties);
};

} // namespace sketch
