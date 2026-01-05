#include "cmd_line_args.h"
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

namespace sketch {

static struct option long_options[] = {
    {"config",       required_argument, 0,  'c' },
    {"interactive",  no_argument,       0,  'i' },
    {0,              0,                 0,  0 }
};

void parse_cmd_line_args(int argc, char** argv, CmdLineArgs& args) {
    int opt;
    int option_index = 0;
    const char* short_options = "ic:";
    while ((opt = getopt_long(argc, argv, short_options, long_options, &option_index)) != -1) {
        switch (opt) {
            case 'i':
                args.interactive = true;
                break;
            case 'c':
                args.config_path = optarg;
                break;
            case '?':
                fprintf(stderr, "Usage: %s [-i] [-c path]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
}

} // namespace sketch