#include "core.h"
#include "config.h"
#include "engine.h"
#include "log.h"

namespace sketch {

Config cfg;
Engine engine(cfg);
//CommandRouter router(engine);

const Config& get_global_config() {
    return cfg;
}

CommandRouter get_command_router() {
    CommandRouter router{engine};
    if (router.init() != 0) {
        LOG_ERROR << "Failed to initialize command router";
    }
    return router;
}

int init_core(const std::string& cfg_file, const std::string& data_path) {
    if (parse_config(cfg_file, cfg) != 0) {
        LOG_ERROR << "Failed to parse config";
        return -1;
    }

    if (!data_path.empty()) {
        cfg.data_path = data_path;
    }

    if (engine.init() != 0) {
        LOG_ERROR << "Failed to initialize engine";
        return -1;
    }

    return 0;
}


} // namespace sketch
