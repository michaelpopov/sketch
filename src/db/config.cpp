#include "config.h"
#include "cmd_line_args.h"
#include "string_utils.h"
#include "log.h"

#include <cstdio>
#include <cstdlib>
#include <cctype>
#include <string>
#include <experimental/scope>

namespace sketch {

int parse_config(const std::string& cfg_file, Config& cfg) {
    FILE* f = fopen(cfg_file.c_str(), "r");
    if (!f) {
        LOG_ERROR << "Failed to open config file: " << cfg_file;
        return -1;
    }

    std::experimental::scope_exit closer([&] {
        fclose(f);
    });

    char line[4096];
    char section[512] = {0};

    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r')) {
            line[--len] = '\0';
        }

        char* s = trim_inplace(line);
        if (!s || *s == '\0') continue;
        if (*s == '#') continue;

        if (s[0] == '[') {
            char* close = strrchr(s, ']');
            if (close) {
                *close = '\0';
                strncpy(section, trim_inplace(s + 1), sizeof(section)-1);
            }
            continue;
        }

        char* eq = strchr(s, '=');
        if (!eq) {
            LOG_ERROR << "Invalid config line: " << s;
            return -1;
        }

        *eq = '\0';
        char* key = trim_inplace(s);
        char* val = trim_inplace(eq + 1);

        if (strcmp(section, "data") == 0) {
            if (strcmp(key, "path") == 0) {
                cfg.data_path = val;
            } else {
                LOG_ERROR << "Unknown config key in [data]: " << key;
            }
        } else if (strcmp(section, "threading") == 0) {
            if (strcmp(key, "thread_pool_size") == 0) {
                cfg.thread_pool_size = static_cast<size_t>(std::strtoul(val, nullptr, 10));
            } else {
                LOG_ERROR << "Unknown config key in [threading]: " << key;
            }
        } else {
            LOG_ERROR << "Unknown config section: " << section;
            return -1;
        }
    }

    return 0;
}

} // namespace sketch