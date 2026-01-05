#include "cmd_line_args.h"
#include "db/core.h"
#include "db/shared_types.h"
#include "db/string_utils.h"
#include "db/log.h"
#include <iostream>
#include <string>
#include <vector>
#include <sstream>

using namespace sketch;

CmdLineArgs args;

int main(int argc, char** argv) {
    parse_cmd_line_args(argc, argv, args);

    if (init_core(args.config_path) != 0) {
        return -1;
    }

    LOG_INFO << "Client start";
    do_work();
    LOG_INFO << "Client finish";

    return 0;
}

namespace sketch {

bool interactive = false;

void do_work() {
    char buffer[256] = { '\0' };
    std::string cmd;
    auto router = get_command_router();

    interactive = args.interactive;

    for (;;) {
        char* line = nullptr;

        if (interactive) {
            line = readline_gets();
        } else {
            std::cin.getline(buffer, sizeof(buffer));
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

        cmd += trim_inplace(line);
        if (cmd.back() == ';') {
            mid_cmd = false;
        } else {
            mid_cmd = true;
            cmd += ' ';
            continue;
        }

        Commands commands;
        parse_command(cmd, commands);

        if (commands.empty()) {
            cmd.clear();
            continue;
        }

        if (commands[0] == "exit" || commands[0] == "quit") {
            break;
        }

        add_history (cmd.c_str());

        Ret ret = router.process_command(commands);

        bool print_message = interactive || ret.is_content();
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

} // namespace sketch
