#include "cmd_line_args.h"
#include "db/core.h"
#include "db/log.h"
#include "db/shared_types.h"
#include "db/string_utils.h"

#include <iostream>
#include <string>
#include <thread>
#include <cstring>
#include <csignal>
#include <atomic>
#include <sstream>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

using namespace sketch;

// Atomic flag to control the main loop
static std::atomic<bool> keep_running(true);
static int global_server_fd = -1;

void report_error(const char* msg) {
    LOG_ERROR << msg << ": " << strerror(errno);
}

void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        LOG_INFO << "\nShutdown signal received. Closing server...";
        keep_running = false;
        
        // Closing the socket will force accept() to return with an error
        if (global_server_fd != -1) {
            shutdown(global_server_fd, SHUT_RDWR);
            close(global_server_fd);
            global_server_fd = -1;
        }
    }
}

std::string process_command(CommandRouter& router, const std::string& cmd) {
    Commands commands;
    parse_command(cmd, commands);
    Ret ret = router.process_command(commands);

    std::stringstream sstream;
    sstream << "0\n"; // Marker for no more data

    std::string prefix = (ret.code() == 0) ? "Ok" : "Error";
    if (ret.message().empty()) {
        sstream << prefix.size() << "\n";
        sstream << prefix;
    } else if (!ret.is_content()){
        sstream << ret.message().size() + prefix.size() + 2 << "\n";
        sstream << prefix << ": " << ret.message() << "\n";
    } else {
        sstream << ret.message().size() << "\n";
        sstream << ret.message();
    }

    return sstream.str();
}

void handle_client(int client_fd) {
    auto router = get_command_router();

    const int BUFFER_SIZE = 1024;
    char buffer[BUFFER_SIZE];

    std::string message;

    while (keep_running) {
        memset(buffer, 0, BUFFER_SIZE);
        buffer[0] = ' ';

        ssize_t bytes_received = recv(client_fd, buffer + 1, BUFFER_SIZE - 2, 0);
        if (bytes_received <= 0) {
            report_error("recv");
            break;
        }

        message.append(buffer, bytes_received + 1);

        size_t pos;
        while ((pos = message.find(';')) != std::string::npos) {
            std::string line = message.substr(0, pos + 1);
            message.erase(0, pos + 1);

            std::string response = process_command(router, line);

            ssize_t bytes_sent = send(client_fd, response.c_str(), response.length(), 0);
            if (bytes_sent <= 0) {
                report_error("send");
                break;
            }
        }
    }

    close(client_fd);
}

void setup_signal_handler() {
    struct sigaction sa;
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
}

int setup_global_server_fd(const char* socket_path) {
    struct sockaddr_un server_addr;
    global_server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (global_server_fd < 0) {
        report_error("socket");
        return 1;
    }

    unlink(socket_path);

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sun_family = AF_UNIX;
    strncpy(server_addr.sun_path, socket_path, sizeof(server_addr.sun_path) - 1);

    if (bind(global_server_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        report_error("bind");
        return 1;
    }

    if (listen(global_server_fd, 5) < 0) {
        report_error("listen");
        return 1;
    }

    LOG_INFO << "Server listening on " << socket_path;
    return 0;
}

int setup_core(const std::string& config_path) {
    if (init_core(config_path) != 0) {
        return 1;
    }

    return 0;
}

int main(int argc, char** argv) {
    CmdLineArgs args;
    parse_cmd_line_args(argc, argv, args);

    int ret = 0;
    ret = setup_core(args.config_path);
    if (ret != 0) {
        return ret;
    }

    setup_signal_handler();
    ret = setup_global_server_fd(args.socket_path.c_str());
    if (ret != 0) {
        return ret;
    }

    while (keep_running) {
        int client_fd = accept(global_server_fd, nullptr, nullptr);
        
        if (client_fd < 0) {
            // If accept failed because we closed the socket in the signal handler
            if (!keep_running) break;
            report_error("accept");
            continue;
        }

        std::thread(handle_client, client_fd).detach();
    }

    // 2. Ordered Cleanup
    LOG_INFO << "Cleaning up socket file...";
    unlink(args.socket_path.c_str());
    LOG_INFO << "Server exited cleanly.";

    return 0;
}
