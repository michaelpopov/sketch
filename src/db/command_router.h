#pragma once
#include "shared_types.h"
#include <memory>
#include <string>

namespace sketch {

class Engine;
class DDLCommandProcessor;
class DataCommandProcessor;

class CommandRouter {
public:
    CommandRouter(Engine& engine);
    CommandRouter(CommandRouter&& another);
    ~CommandRouter();
    Ret init();
    Ret process_command(Commands& commands);
    Ret process_command(const std::string& cmd);

    // For testing purposes
    DataCommandProcessor& dcp() { return *data_command_processor_; }

private:
    Engine& engine_;
    std::unique_ptr<DDLCommandProcessor> ddl_command_processor_;
    std::unique_ptr<DataCommandProcessor> data_command_processor_;
    CommandNames ddl_commands_;
    CommandNames data_commands_;

};

} // namespace sketch