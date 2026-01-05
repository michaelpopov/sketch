#pragma once
#include <string>

namespace sketch {

struct CmdLineArgs {
    std::string config_path;
    bool interactive = false;
    bool clean = false;
    std::string extend;
    bool show = false;
};

void parse_cmd_line_args(int argc, char** argv, CmdLineArgs& args);

} // namespace sketch