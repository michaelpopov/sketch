
#include "command_runner.h"
#include "db/core.h"
#include "db/string_utils.h"
#include <experimental/scope>
#include <iostream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace sketch {

extern bool interactive; // defined in client.cpp
static int counter = 0;
void execute(CommandRouter& router, FILE* in, FILE* out);

void execute_thread(const std::string& command, bool is_file) {
    if (!is_file) {
        counter++;
    }

    std::thread backgroundWorker([command, is_file]() {
        FILE* out = nullptr;
        FILE* pipe = nullptr;
        const std::experimental::scope_exit out_closer([&] {
            if (pipe) {
                if (is_file) {
                    fclose(pipe);
                } else {
                    pclose(pipe);
                }
            }

            if (out && out != stdout) {
                fclose(out);
            }
        });

        if (is_file) {
            out = stdout;
            pipe = fopen(command.c_str(), "r");
            if (pipe == nullptr) {
                std::cerr << "Failed to open input file: [" << command << "]" <<std::endl;
                return;
            }
        } else {
            std::string output = std::string("output_") + std::to_string(counter) + ".txt";
            out = fopen(output.c_str(), "w");
            if (out == nullptr) {
                std::cerr << "Failed to open output file: " << output << std::endl;
                return;
            }

            auto cmd = command;
            if (cmd.find('/') == std::string::npos) {
                cmd = std::string("./") + cmd;
            }
            std::string fullCommand = cmd + " 2>&1";
            pipe = popen(fullCommand.c_str(), "r");
            if (pipe == nullptr) {
                std::cerr << "Failed to run command: " << fullCommand << std::endl;
                return;
            }
        }

        CommandRouter router = get_command_router();
        execute(router, pipe, out);
    });

    if (is_file) {
        backgroundWorker.join();
    } else {
        backgroundWorker.detach();
    }
}

void execute(CommandRouter& router, FILE* in, FILE* out) {
    std::string cmd;
    std::vector<char> buffer(16*1024);

    while (fgets(buffer.data(), buffer.size(), in) != nullptr) {
        char* line = trim_inplace(buffer.data());

        int len = strlen(line);
        if (len > 0 && line[len - 1] == '\n') {
            line[len - 1] = '\0';
        }
        if (!*line) {
            continue;
        }

        cmd += trim_inplace(line);

        Commands commands;
        parse_command(cmd, commands);

        if (commands.empty()) {
            cmd.clear();
            continue;
        }

        fprintf(out, "> %s\n", line);

        Ret ret = router.process_command(commands);

        bool print_message = interactive || ret.is_content();
        if (!print_message) {
            fprintf(out,  "%d\n", ret.code());
        } else {
            fprintf(out,  "%s\n", ret.message().c_str());

            bool extra_newline = !ret.message().empty() && ret.message().back() != '\n';
            if (extra_newline) {
                fprintf(out, "\n");
            }
        }

        cmd.clear();
    }
}

} // namespace sketch
