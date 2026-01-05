#pragma once
#include "command_router.h"

namespace sketch {

int init_core(const std::string& cfg_file, const std::string& data_path = "");
CommandRouter get_command_router();

} // namespace sketch