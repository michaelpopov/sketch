#include "ddl_command_processor.h"
#include "engine.h"
#include "string_utils.h"
#include "log.h"
#include <format>
#include <iostream>

namespace sketch {

static CommandNames supported_commands = { "CREATE", "DROP", "LIST", "SHOW", "DUMMY" };

Ret DDLCommandProcessor::process_command(Commands& commands, bool is_help) {
    if (commands.empty()) {
        return "No command to process";
    }

    const auto& cmd_type = commands[0];

    if (cmd_type == "CREATE") {
        return process_create_cmd(commands, is_help);
    } else if (cmd_type == "DROP") {
        return process_drop_cmd(commands, is_help);
    } else if (cmd_type == "LIST") {
        return process_list_cmd(commands, is_help);
    } else if (cmd_type == "SHOW") {
        return process_show_cmd(commands, is_help);
    } else if (cmd_type == "DUMMY") {
        return process_dummy_cmd(commands, is_help);
    }

    return std::string("Unknown DDL command type: ") + std::string(cmd_type);
}

const CommandNames& DDLCommandProcessor::get_supported_commands() const {
    return supported_commands;
}

Ret DDLCommandProcessor::process_dummy_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "DUMMY command help: DUMMY <any set of parameters>");
    }

    std::string output;
    for (const auto& s: commands) {
        output += s;
        output += ' ';
    }

    return Ret(0, output);
}

Ret DDLCommandProcessor::process_create_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        if (commands.back() == "CATALOG") {
            return Ret(0, "CREATE command help: CREATE CATALOG <catalog_name>");
        } else if (commands.back() == "DATASET") {
            return Ret(0, "CREATE command help: CREATE DATASET <catalog_name>.<dataset_name> [TYPE = <f32|f16>] [DIM = <dim>] [COUNT = <nodes_count>]");
        }
        return "CREATE command help: CREATE CATALOG or CREATE DATASET";
    }

    if (commands.size() < 3) {
        return "CREATE command requires additional parameters";
    }

    if (commands[1] == "CATALOG") {
        CmdCreateCatalog cmd { .catalog_name = commands[2] };
        if (!is_valid_identifier(cmd.catalog_name)) {
            return std::string("Invalid catalog name: ") + std::string(cmd.catalog_name);
        }
        return engine_.create_catalog(cmd);
    } else if (commands[1] == "DATASET") {
        std::vector<std::string_view> parts;
        split_string(commands[2], '.', parts);
        if (parts.size() != 2) {
            return "Dataset name must be in the format <catalog_name>.<dataset_name>";
        }
        if (!is_valid_identifier(parts[1])) {
            return std::string("Invalid dataset name: ") + std::string(parts[1]);
        }
        CmdCreateDataset cmd {
            .catalog_name = parts[0],
            .dataset_name = parts[1],
            .type = DatasetType::f32,
            .dim = 1024,
            .nodes_count = 1
        };
        Properties properties;
        int ret = get_properties_from_command(commands, 3, properties);
        if (ret != 0) {
            return "Failed to parse dataset properties";
        }

        size_t properties_count = 0;
        auto prop_iter = properties.find("TYPE");
        if (prop_iter != properties.end()) {
            properties_count++;
            if (prop_iter->second == "f32") {
                cmd.type = DatasetType::f32;
            } else if (prop_iter->second == "f16") {
                cmd.type = DatasetType::f16;
            } else {
                return std::format("Unsupported TYPE value: '{}'", prop_iter->second);
            }
        }
        prop_iter = properties.find("DIM");
        if (prop_iter != properties.end()) {
            properties_count++;
            PARAM_CONV(cmd.dim, prop_iter->second);
        }
        prop_iter = properties.find("NODES");
        if (prop_iter != properties.end()) {
            properties_count++;
            PARAM_CONV(cmd.nodes_count, prop_iter->second);
        }
        if (properties_count != properties.size()) {
            return "Unknown properties provided for CREATE DATASET";
        }

        return engine_.create_dataset(cmd);
    }

    return std::string("Unknown CREATE command type: ") + std::string(commands[1]);
}

Ret DDLCommandProcessor::process_drop_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        if (commands.back() == "CATALOG") {
            return Ret(0, "DROP command help: DROP CATALOG <catalog_name>");
        } else if (commands.back() == "DATASET") {
            return Ret(0, "DROP command help: DROP DATASET <catalog_name>.<dataset_name>");
        }
        return Ret(0, "DROP command help: DROP CATALOG or DROP DATASET");
    }

    if (commands.size() < 3) {
        return "DROP command requires additional parameters";
    }

    if (commands[1] == "CATALOG") {
        return engine_.drop_catalog(CmdDropCatalog{ .catalog_name = commands[2] });
    } else if (commands[1] == "DATASET") {
        std::vector<std::string_view> parts;
        split_string(commands[2], '.', parts);
        if (parts.size() != 2) {
            return "Dataset name must be in the format <catalog_name>.<dataset_name>";
        }
        CmdDropDataset cmd { .catalog_name = parts[0], .dataset_name = parts[1] };
        return engine_.drop_dataset(cmd);
    }

    return std::string("Unknown DROP command type: ") + std::string(commands[1]);
}

Ret DDLCommandProcessor::process_list_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "LIST command help: LIST CATALOGS or LIST DATASETS <catalog_name>");
    }

    if (commands.size() < 2) {
        return "LIST command requires additional parameters";
    }

    if (commands[1] == "CATALOGS") {
        return engine_.list_catalogs(CmdListCatalogs{});
    } else if (commands[1] == "DATASETS") {
        if (commands.size() < 3) {
            return "LIST DATASETS command requires additional parameters";
        }
        CmdListDatasets cmd { .catalog_name = commands[2] };
        return engine_.list_datasets(cmd);
    }

    return std::string("Unknown LIST command type: ") + std::string(commands[1]);
}

Ret DDLCommandProcessor::process_show_cmd(Commands& commands, bool is_help) {
    if (is_help) {
        return Ret(0, "SHOW command help: SHOW DATASET <catalog_name>.<dataset_name>");
    }

    if (commands.size() < 3) {
        return "SHOW command requires additional parameters";
    }

    if (commands[1] == "DATASET") {
        std::vector<std::string_view> parts;
        split_string(commands[2], '.', parts);
        if (parts.size() != 2) {
            return "Dataset name must be in the format <catalog_name>.<dataset_name>";
        }
        CmdShowDataset cmd { .catalog_name = parts[0], .dataset_name = parts[1] };
        return engine_.show_dataset(cmd);
    }

    return std::string("Unknown SHOW command type: ") + std::string(commands[1]);
}


int DDLCommandProcessor::get_properties_from_command(Commands& commands, size_t from_index, Properties& properties) {
    while (from_index < commands.size()) {
        const auto& name = commands[from_index];

        from_index++;
        if (from_index >= commands.size() || commands[from_index] != "=") {
            return -1;
        }

        from_index++;
        if (from_index >= commands.size()) {
            return -1;
        }

        properties[std::string(name)] = std::string(commands[from_index]);
        from_index++;
    }

    return 0;
}

} // namespace sketch