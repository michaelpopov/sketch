#include "cmd_line_args.h"
#include "command_runner.h"
#include "db/config.h"
#include "db/core.h"
#include "db/shared_types.h"
#include "db/string_utils.h"
#include "db/log.h"
#include <readline/readline.h>
#include <readline/history.h>
#include <experimental/scope>
#include <filesystem>
#include <iostream>
#include <string>
#include <vector>
#include <sstream>

using namespace sketch;

namespace sketch {

static char *line_read = nullptr;
static bool mid_cmd = false;
static constexpr const char* full_prompt = "> ";
static constexpr const char* partial_prompt = "~ ";

static void do_work(const std::string& data_dir);
static char* readline_gets();
void replace_all(std::string& s,
                 const std::string& from,
                 const std::string& to);
std::string get_data_dir();

} // namespace sketch

CmdLineArgs args;

int main(int argc, char** argv) {
    parse_cmd_line_args(argc, argv, args);

    std::string data_dir = get_data_dir();
    if (!args.extend.empty()) {
        data_dir += "/";
        data_dir += args.extend;
        data_dir += "/";
    }
    if (args.clean) {
        if (std::filesystem::exists(data_dir)) {
            std::filesystem::remove_all(data_dir);
        }
        std::filesystem::create_directories(data_dir);
    }
    const std::experimental::scope_exit closer([&] {
        if (args.clean && std::filesystem::exists(data_dir)) {
            std::filesystem::remove_all(data_dir);
        }
    });


    if (init_core(args.config_path, data_dir) != 0) {
        return -1;
    }

    do_work(data_dir);

    return 0;
}

namespace sketch {

bool interactive = false;

void do_work(const std::string& data_dir) {
    char buffer[256] = { '\0' };
    std::string cmd;
    auto router = get_command_router();

    interactive = args.interactive;

    for (;;) {
        char* line = nullptr;

        if (interactive) {
            line = readline_gets();
        } else {
            if (!std::cin.getline(buffer, sizeof(buffer))) {
                break;
            }
            line = buffer;
        }

        if (!line || !*line) {
            continue;
        }

        if (strcmp(line, "exit") == 0 ||
            strcmp(line, "quit") == 0){
            break;
        }

        int len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }

        if (len > 2 && line[0] == '#' && line[1] == '!') {
            execute_thread(trim_inplace(line + 2));
            continue;
        }

        if (len > 2 && line[0] == '#' && line[1] == '@') {
            execute_thread(trim_inplace(line + 2), true);
            continue;
        }

        if (len > 2 && line[0] == '#' && line[1] == '^') {
            std::string cmd = trim_inplace(line + 2);
            replace_all(cmd, "$DIR", data_dir);
            system(cmd.c_str());
            continue;
        }

        if (len > 0 && line[0] == '#') {
            continue;
        }

        cmd += trim_inplace(line);
        if (cmd.back() == ';') {
            mid_cmd = false;
        } else {
            mid_cmd = true;
            cmd += ' ';
            continue;
        }

        replace_all(cmd, "$DIR", data_dir);

        Commands commands;
        parse_command(cmd, commands);

        if (commands.empty()) {
            cmd.clear();
            continue;
        }

        if (commands[0] == "exit" || commands[0] == "quit") {
            break;
        }

        if (interactive) {
            add_history (cmd.c_str());
        }

        Ret ret = router.process_command(commands);

        bool print_message = interactive || ret.is_content() || args.show;
        if (!print_message) {
            std::cout << ret.code() << std::endl;
        } else {
            std::cout << ret.message() << std::endl;

            bool extra_newline = !ret.message().empty() && ret.message().back() != '\n';
            if (extra_newline) {
                std::cout << std::endl;
            }
        }

        cmd.clear();
    }
}

char* readline_gets() {
    // If the buffer has already been allocated, return the memory to the free pool.
    if (line_read) {
        free (line_read);
        line_read = nullptr;
    }

    const char* prompt = mid_cmd ? partial_prompt : full_prompt;
    line_read = readline (prompt);
 
    return line_read;
}

void replace_all(std::string& s,
                 const std::string& from,
                 const std::string& to)
{
    if (from.empty()) return; // avoid infinite loop

    std::size_t pos = 0;
    while ((pos = s.find(from, pos)) != std::string::npos) {
        s.replace(pos, from.length(), to);
        pos += to.length();
    }
}

std::string get_data_dir() {
    Config cfg;
    if (parse_config(args.config_path, cfg) != 0) {
        LOG_ERROR << "Failed to parse config";
        exit(-1);
    }
    return cfg.data_path;
}

} // namespace sketch
