#include "cmd_line_args.h"
#include "db/log.h"
#include "db/string_utils.h"

#include <readline/readline.h>
#include <readline/history.h>

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>

#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

using namespace sketch;
namespace sketch {

static char *line_read = nullptr;
static bool mid_cmd = false;
static constexpr const char* full_prompt = "> ";
static constexpr const char* partial_prompt = "~ ";

static void do_work();
static char* readline_gets();

} // namespace sketch

CmdLineArgs args;

const int BUFFER_SIZE = 1024;
static int sock_fd = -1;

void report_error(const char* msg) {
    LOG_ERROR << msg << ": " << strerror(errno);
}

int setup_connection(const char* socket_path) {
    struct sockaddr_un server_addr;

    sock_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        report_error("socket");
        return 1;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sun_family = AF_UNIX;
    strncpy(server_addr.sun_path, socket_path, sizeof(server_addr.sun_path) - 1);

    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        report_error("connect");
        close(sock_fd);
        return 1;
    }

    return 0;
}

int recv_message(int& marker, std::vector<char>& buffer) {
     memset(buffer.data(), 0, buffer.size());
    ssize_t bytes_received = recv(sock_fd, buffer.data(), buffer.size() - 1, 0);
    if (bytes_received <= 0) {
        return bytes_received;
    }

    // Simplified assumption: marker and length arrive together in the beginning.

    auto pos = std::find(buffer.data(), buffer.data() + bytes_received, '\n');
    if (pos == buffer.data() + bytes_received) {
        // Marker delimiter not found
        return -1;
    }

    std::string_view marker_str(buffer.data(), pos - buffer.data());
    marker = std::stoi(std::string(marker_str));

   auto pos2 = std::find(pos + 1, buffer.data() + bytes_received, '\n');
    if (pos2 == buffer.data() + bytes_received) {
        // Length delimiter not found
        return -1;
    }

    std::string_view length_str(pos + 1, pos2 - (pos + 1));
    uint32_t msg_len = std::stoi(std::string(length_str));

    const auto from = pos2 + 1;
    const uint64_t header_len = from - buffer.data();

    if (msg_len + header_len > buffer.size()) {
        return -1;
    }

    uint64_t offset = bytes_received;
    while (offset < msg_len + header_len) {
        bytes_received = recv(sock_fd, buffer.data() + offset, buffer.size() - offset, 0);
        if (bytes_received <= 0) {
            return bytes_received;
        }

        offset += bytes_received;
    }

    std::string response(buffer.data() + header_len, msg_len);
    std::cout << response << std::endl;
    if (response.back() != '\n') {
        std::cout << std::endl;
    }

    return 0;
}

int send_recv(const std::string& request) {
    if (send(sock_fd, request.c_str(), request.length(), 0) < 0) {
        report_error("send");
        return 1;
    }

    std::vector<char> buffer(16 * 1024);
    int marker = 0;
    do {
        try {
            int ret = recv_message(marker, buffer);
            if (ret != 0) {
                report_error("recv");
                return ret;
            }
        } catch (const std::exception& ex) {
            LOG_ERROR << "Exception while receiving message: " << ex.what();
            return 1;
        }
    } while (marker > 0);

    return 0;
}

int main(int argc, char* argv[]) {
    parse_cmd_line_args(argc, argv, args);

    int ret = setup_connection(args.sock_path.c_str());
    if (ret != 0) {
        return ret;
    }

    do_work();
    close(sock_fd);
    return 0;
}

namespace sketch {

void do_work() {
    char buffer[256] = { '\0' };
    std::string cmd;

    for (;;) {
        char* line = nullptr;

        if (args.interactive) {
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

        cmd += trim_inplace(line);
        if (cmd.back() == ';') {
            mid_cmd = false;
        } else {
            mid_cmd = true;
            cmd += ' ';
            continue;
        }

        if (cmd == "exit" || cmd == "quit") {
            break;
        }

        add_history (cmd.c_str());

        int ret = send_recv(cmd);
        if (ret != 0) {
            break;
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