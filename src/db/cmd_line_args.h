#pragma once
#include <string>

namespace sketch {

struct CmdLineArgs {
    std::string config_path;
    bool interactive = false;
};

void parse_cmd_line_args(int argc, char** argv, CmdLineArgs& args);

} // namespace sketch