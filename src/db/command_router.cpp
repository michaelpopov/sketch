#include "command_router.h"
#include "ddl_command_processor.h"
#include "data_command_processor.h"
#include "string_utils.h"
#include "log.h"
#include <assert.h>
#include <unordered_set>
#include <iostream>

namespace sketch {

CommandRouter::CommandRouter(Engine& engine)
  : engine_(engine),
    ddl_command_processor_(std::make_unique<DDLCommandProcessor>(engine)),
    data_command_processor_(std::make_unique<DataCommandProcessor>(engine))
{
}

CommandRouter::CommandRouter(CommandRouter&& another)
  : engine_(another.engine_),
    ddl_command_processor_(std::move(another.ddl_command_processor_)),
    data_command_processor_(std::move(another.data_command_processor_)),
    ddl_commands_(std::move(another.ddl_commands_)),
    data_commands_(std::move(another.data_commands_))
{
}

CommandRouter::~CommandRouter() {
    // No-op. Needed for unique_ptr members.
}

Ret CommandRouter::init() {
    ddl_commands_ = ddl_command_processor_->get_supported_commands();
    data_commands_ = data_command_processor_->get_supported_commands();
    return 0;
}

Ret CommandRouter::process_command(Commands& commands) {
    if (commands.empty()) {
        return "Invalid empty command.";
    }

    if (commands.back() == ";") {
        commands.pop_back(); // Remove trailing ';'
    }

    if (commands.empty()) {
        return "no-op";
    }

    auto cmd = commands[0];

    bool is_help = false;
    if (cmd == "HELP") {
        Commands temp_commands;
        for (size_t i = 1; i < commands.size(); i++) {
            temp_commands.push_back(commands[i]);
        }
        commands = std::move(temp_commands);
        cmd = commands[0];
        is_help = true;
    }

    if (ddl_commands_.find(commands[0]) != ddl_commands_.end()) {
        return ddl_command_processor_->process_command(commands, is_help);
    } else if (data_commands_.find(commands[0]) != data_commands_.end()) {
        return data_command_processor_->process_command(commands, is_help);
    }

    return std::string("Unknown command: ") + std::string(commands[0]);
}

Ret CommandRouter::process_command(const std::string& cmd) {
    Commands commands;
    parse_command(cmd, commands);
    return process_command(commands);
}


} // namespace sketch