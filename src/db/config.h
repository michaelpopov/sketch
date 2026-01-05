#pragma once
#include <string>

namespace sketch {

struct Config {
    std::string data_path;
};

int parse_config(const std::string& cfg_file, Config& cfg);
const Config& get_global_config();

} // namespace sketch