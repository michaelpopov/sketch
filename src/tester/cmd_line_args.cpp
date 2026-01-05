#include "cmd_line_args.h"
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>

namespace sketch {

static struct option long_options[] = {
    {"config",       required_argument, 0,  'c' },
    {"clean",        no_argument,       0,  'n' },
    {"extend",       required_argument, 0,  'e' },
    {"interactive",  no_argument,       0,  'i' },
    {"show",         no_argument,       0,  's' },
    {0,              0,                 0,  0 }
};

void parse_cmd_line_args(int argc, char** argv, CmdLineArgs& args) {
    int opt;
    int option_index = 0;
    const char* short_options = "sine:c:";
    while ((opt = getopt_long(argc, argv, short_options, long_options, &option_index)) != -1) {
        switch (opt) {
            case 'i':
                args.interactive = true;
                break;
            case 's':
                args.show = true;
                break;
            case 'c':
                args.config_path = optarg;
                break;
            case 'n':
                args.clean = true;
                break;
            case 'e':
                args.extend = optarg;
                break;
            case '?':
                fprintf(stderr, "Usage: %s [-i] [-n] [-c path]\n", argv[0]);
                exit(EXIT_FAILURE);
        }
    }
}

} // namespace sketch