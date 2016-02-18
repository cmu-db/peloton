/* Copyright (c) 2012 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * This is a basic end-to-end test of LogCabin.
 */

#include <cassert>
#include <cstdlib>
#include <getopt.h>
#include <iostream>

#include <google/protobuf/stubs/common.h>
#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>

namespace {

using LogCabin::Client::Cluster;
using LogCabin::Client::Result;
using LogCabin::Client::Status;
using LogCabin::Client::Tree;

/**
 * Parses argv for the main function.
 */
class OptionParser {
  public:
    OptionParser(int& argc, char**& argv)
        : argc(argc)
        , argv(argv)
        , cluster("logcabin:5254")
        , logPolicy("")
        , mock(false)
    {
        while (true) {
            static struct option longOptions[] = {
               {"cluster",  required_argument, NULL, 'c'},
               {"mock",  no_argument, NULL, 'm'},
               {"help",  no_argument, NULL, 'h'},
               {"verbose",  no_argument, NULL, 'v'},
               {"verbosity",  required_argument, NULL, 256},
               {0, 0, 0, 0}
            };
            int c = getopt_long(argc, argv, "c:hmv", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c) {
                case 'c':
                    cluster = optarg;
                    break;
                case 'h':
                    usage();
                    exit(0);
                case 'm':
                    mock = true;
                    break;
                case 'v':
                    logPolicy = "VERBOSE";
                    break;
                case 256:
                    logPolicy = optarg;
                    break;
                case '?':
                default:
                    // getopt_long already printed an error message.
                    usage();
                    exit(1);
            }
        }
    }

    void usage() {
        std::cout
            << "Runs an extremely basic test against LogCabin, useful as a "
            << "quick sanity check."
            << std::endl
            << std::endl
            << "This program is subject to change (it is not part of "
            << "LogCabin's stable API)."
            << std::endl
            << std::endl

            << "Usage: " << argv[0] << " [options]"
            << std::endl
            << std::endl

            << "Options:"
            << std::endl

            << "  -c <addresses>, --cluster=<addresses>  "
            << "Network addresses of the LogCabin"
            << std::endl
            << "                                         "
            << "servers, comma-separated"
            << std::endl
            << "                                         "
            << "[default: logcabin:5254]"
            << std::endl

            << "  -h, --help                     "
            << "Print this usage information"
            << std::endl

            << "  -m, --mock                     "
            << "Instead of connecting to a LogCabin cluster,"
            << std::endl
            << "                                 "
            << "use a client-local, in-memory data structure"
            << std::endl

            << "  -v, --verbose                  "
            << "Same as --verbosity=VERBOSE"
            << std::endl

            << "  --verbosity=<policy>           "
            << "Set which log messages are shown."
            << std::endl
            << "                                 "
            << "Comma-separated LEVEL or PATTERN@LEVEL rules."
            << std::endl
            << "                                 "
            << "Levels: SILENT ERROR WARNING NOTICE VERBOSE."
            << std::endl
            << "                                 "
            << "Patterns match filename prefixes or suffixes."
            << std::endl
            << "                                 "
            << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE."
            << std::endl;
    }

    int& argc;
    char**& argv;
    std::string cluster;
    std::string logPolicy;
    bool mock;
};

} // anonymous namespace

int
main(int argc, char** argv)
{
    try {

        atexit(google::protobuf::ShutdownProtobufLibrary);
        OptionParser options(argc, argv);
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                options.logPolicy));
        Cluster cluster = (options.mock
            ? Cluster(std::make_shared<LogCabin::Client::TestingCallbacks>())
            : Cluster(options.cluster));

        Tree tree = cluster.getTree();
        tree.makeDirectoryEx("/etc");
        tree.writeEx("/etc/passwd", "ha");
        std::string contents = tree.readEx("/etc/passwd");
        assert(contents == "ha");

        // need to write a kilobyte for snapshot to be taken under default
        // settings
        std::string laughter;
        for (int i = 0; i < 512; ++i)
            laughter += "ha";
        tree.writeEx("/etc/lol", laughter);

        tree.removeDirectoryEx("/etc");
        return 0;

    } catch (const LogCabin::Client::Exception& e) {
        std::cerr << "Exiting due to LogCabin::Client::Exception: "
                  << e.what()
                  << std::endl;
        exit(1);
    }
}
