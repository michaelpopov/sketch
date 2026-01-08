#pragma once
#include <string>

namespace sketch {

struct Config {
    std::string data_path;
    size_t thread_pool_size = 0;
};

int parse_config(const std::string& cfg_file, Config& cfg);
const Config& get_global_config();

} // namespace sketch