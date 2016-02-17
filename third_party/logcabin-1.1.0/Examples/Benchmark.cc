/* Copyright (c) 2012-2014 Stanford University
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
 * This is a basic latency/bandwidth benchmark of LogCabin.
 */

// std::atomic header file renamed in gcc 4.5.
// Clang uses <atomic> but has defines like gcc 4.2.
#if __GNUC__ == 4 && __GNUC_MINOR__ < 5 && !__clang__
#include <cstdatomic>
#else
#include <atomic>
#endif
#include <cassert>
#include <ctime>
#include <getopt.h>
#include <iostream>
#include <thread>
#include <unistd.h>

#include <LogCabin/Client.h>
#include <LogCabin/Debug.h>
#include <LogCabin/Util.h>

namespace {

using LogCabin::Client::Cluster;
using LogCabin::Client::Result;
using LogCabin::Client::Status;
using LogCabin::Client::Tree;
using LogCabin::Client::Util::parseNonNegativeDuration;

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
        , size(1024)
        , writers(1)
        , totalWrites(1000)
        , timeout(parseNonNegativeDuration("30s"))
    {
        while (true) {
            static struct option longOptions[] = {
               {"cluster",  required_argument, NULL, 'c'},
               {"help",  no_argument, NULL, 'h'},
               {"size",  required_argument, NULL, 's'},
               {"threads",  required_argument, NULL, 't'},
               {"timeout",  required_argument, NULL, 'd'},
               {"writes",  required_argument, NULL, 'w'},
               {"verbose",  no_argument, NULL, 'v'},
               {"verbosity",  required_argument, NULL, 256},
               {0, 0, 0, 0}
            };
            int c = getopt_long(argc, argv, "c:hs:t:w:v", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c) {
                case 'c':
                    cluster = optarg;
                    break;
                case 'd':
                    timeout = parseNonNegativeDuration(optarg);
                    break;
                case 'h':
                    usage();
                    exit(0);
                case 's':
                    size = uint64_t(atol(optarg));
                    break;
                case 't':
                    writers = uint64_t(atol(optarg));
                    break;
                case 'w':
                    totalWrites = uint64_t(atol(optarg));
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
            << "Writes repeatedly to LogCabin. Stops once it reaches "
            << "the given number of"
            << std::endl
            << "writes or the timeout, whichever comes first."
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

            << "  -h, --help              "
            << "Print this usage information"
            << std::endl

            << "  --size <bytes>          "
            << "Size of value in each write [default: 1024]"
            << std::endl

            << "  --threads <num>         "
            << "Number of concurrent writers [default: 1]"
            << std::endl

            << "  --timeout <time>        "
            << "Time after which to exit [default: 30s]"
            << std::endl

            << "  --writes <num>          "
            << "Number of total writes [default: 1000]"
            << std::endl

            << "  -v, --verbose           "
            << "Same as --verbosity=VERBOSE"
            << std::endl

            << "  --verbosity=<policy>    "
            << "Set which log messages are shown."
            << std::endl
            << "                          "
            << "Comma-separated LEVEL or PATTERN@LEVEL rules."
            << std::endl
            << "                          "
            << "Levels: SILENT, ERROR, WARNING, NOTICE, VERBOSE."
            << std::endl
            << "                          "
            << "Patterns match filename prefixes or suffixes."
            << std::endl
            << "                          "
            << "Example: Client@NOTICE,Test.cc@SILENT,VERBOSE."
            << std::endl;
    }

    int& argc;
    char**& argv;
    std::string cluster;
    std::string logPolicy;
    uint64_t size;
    uint64_t writers;
    uint64_t totalWrites;
    uint64_t timeout;
};

/**
 * The main function for a single client thread.
 * \param id
 *      Unique ID for this thread, counting from 0.
 * \param options
 *      Arguments describing benchmark.
 * \param tree
 *      Interface to LogCabin.
 * \param key
 *      Key to write repeatedly.
 * \param value
 *      Value to write at key repeatedly.
 * \param exit
 *      When this becomes true, this thread should exit.
 * \param[out] writesDone
 *      The number of writes this thread has completed.
 */
void
writeThreadMain(uint64_t id,
                const OptionParser& options,
                Tree tree,
                const std::string& key,
                const std::string& value,
                std::atomic<bool>& exit,
                uint64_t& writesDone)
{
    uint64_t numWrites = options.totalWrites / options.writers;
    // assign any odd leftover writes in a balanced way
    if (options.totalWrites - numWrites * options.writers > id)
        numWrites += 1;
    for (uint64_t i = 0; i < numWrites; ++i) {
        if (exit)
            break;
        tree.writeEx(key, value);
        writesDone = i + 1;
    }
}

/**
 * Return the time since the Unix epoch in nanoseconds.
 */
uint64_t timeNanos()
{
    struct timespec now;
    int r = clock_gettime(CLOCK_REALTIME, &now);
    assert(r == 0);
    return uint64_t(now.tv_sec) * 1000 * 1000 * 1000 + uint64_t(now.tv_nsec);
}

/**
 * Main function for the timer thread, whose job is to wait until a particular
 * timeout elapses and then set 'exit' to true.
 * \param timeout
 *      Seconds to wait before setting exit to true.
 * \param[in,out] exit
 *      If this is set to true from another thread, the timer thread will exit
 *      soonish. Also, if the timeout elapses, the timer thread will set this
 *      to true and exit.
 */
void
timerThreadMain(uint64_t timeout, std::atomic<bool>& exit)
{
    uint64_t start = timeNanos();
    while (!exit) {
        usleep(50 * 1000);
        if ((timeNanos() - start) > timeout) {
            exit = true;
        }
    }
}

} // anonymous namespace

int
main(int argc, char** argv)
{
    try {

        OptionParser options(argc, argv);
        LogCabin::Client::Debug::setLogPolicy(
            LogCabin::Client::Debug::logPolicyFromString(
                options.logPolicy));
        Cluster cluster = Cluster(options.cluster);
        Tree tree = cluster.getTree();

        std::string key("/bench");
        std::string value(options.size, 'v');

        uint64_t startNanos = timeNanos();
        std::atomic<bool> exit(false);
        std::vector<uint64_t> writesDonePerThread(options.writers);
        uint64_t totalWritesDone = 0;
        std::vector<std::thread> threads;
        std::thread timer(timerThreadMain, options.timeout, std::ref(exit));
        for (uint64_t i = 0; i < options.writers; ++i) {
            threads.emplace_back(writeThreadMain, i, std::ref(options),
                                 tree, std::ref(key), std::ref(value),
                                 std::ref(exit),
                                 std::ref(writesDonePerThread.at(i)));
        }
        for (uint64_t i = 0; i < options.writers; ++i) {
            threads.at(i).join();
            totalWritesDone += writesDonePerThread.at(i);
        }
        uint64_t endNanos = timeNanos();
        exit = true;
        timer.join();

        tree.removeFile(key);
        std::cout << "Benchmark took "
                  << static_cast<double>(endNanos - startNanos) / 1e6
                  << " ms to write "
                  << totalWritesDone
                  << " objects"
                  << std::endl;
        return 0;

    } catch (const LogCabin::Client::Exception& e) {
        std::cerr << "Exiting due to LogCabin::Client::Exception: "
                  << e.what()
                  << std::endl;
        exit(1);
    }
}
