/*-------------------------------------------------------------------------
 *
 * main.cc
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/main.cc
 *
 *-------------------------------------------------------------------------
 */

#include "nstore.h"
#include "common/logger.h"

#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

namespace nstore {

static void usage_exit() {
	std::cerr << "CLI usage : nstore <args> \n"
			<< "   -h --help       				:  Print help message \n"
			<< "   -f --filesystem-path    		:  Path for Filesystem \n";

	exit(EXIT_FAILURE);
}

static struct option opts[] = {
		{ "filesystem-path", optional_argument, NULL, 'f' },
		{ NULL, 0, NULL, 0 }
};

static void parse_arguments(int argc, char* argv[], configuration& config) {
	// Parse arguments

	while (1) {
		int idx = 0;
		int c = getopt_long(argc, argv, "fh", opts, &idx);

		if (c == -1)
			break;

		switch (c) {

		case 'f':
			config.filesystem_path = std::string(optarg);
			std::cout << "filesystem_path        :: " << config.filesystem_path << std::endl;
			break;

		case 'h':
			usage_exit();
			break;

		default:
			std::cerr <<"unknown option: --" << (char) c << "-- \n";
			usage_exit();
		}
	}
}

}

INITIALIZE_EASYLOGGINGPP

int main(int argc, char **argv) {
	nstore::configuration state;

	nstore::parse_arguments(argc, argv, state);

	// Start logger
	nstore::Logger();
	LOG(INFO) << "Starting nstore";

	return 0;
}

