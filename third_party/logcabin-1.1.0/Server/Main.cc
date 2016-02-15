/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include <getopt.h>
#include <unistd.h>

#include <iostream>
#include <string>

#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"

namespace {

/**
 * Parses argv for the main function.
 */
class OptionParser {
  public:
    OptionParser(int& argc, char**& argv)
        : argc(argc)
        , argv(argv)
        , bootstrap(false)
        , configFilename("logcabin.conf")
        , daemon(false)
        , debugLogFilename() // empty for default
        , pidFilename() // empty for none
        , testConfig(false)
    {
        while (true) {
            static struct option longOptions[] = {
               {"bootstrap",  no_argument, NULL, 'b'},
               {"config",  required_argument, NULL, 'c'},
               {"daemon",  no_argument, NULL, 'd'},
               {"help",  no_argument, NULL, 'h'},
               {"log",  required_argument, NULL, 'l'},
               {"pidfile",  required_argument, NULL, 'p'},
               {"test",  no_argument, NULL, 't'},
               {0, 0, 0, 0}
            };
            int c = getopt_long(argc, argv, "bc:dhl:p:t", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c) {
                case 'h':
                    usage();
                    exit(0);
                case 'b':
                    bootstrap = true;
                    break;
                case 'c':
                    configFilename = optarg;
                    break;
                case 'd':
                    daemon = true;
                    break;
                case 'l':
                    debugLogFilename = optarg;
                    break;
                case 'p':
                    pidFilename = optarg;
                    break;
                case 't':
                    testConfig = true;
                    break;
                case '?':
                default:
                    // getopt_long already printed an error message.
                    usage();
                    exit(1);
            }
        }

        // We don't expect any additional command line arguments (not options).
        if (optind != argc) {
            usage();
            exit(1);
        }
    }

    void usage() {
        std::cout
            << "Runs a LogCabin server."
            << std::endl
            << std::endl
            << "This program was released in LogCabin v1.0.0."
            << std::endl
            << std::endl

            << "Usage: " << argv[0] << " [options]"
            << std::endl
            << std::endl

            << "Options:"
            << std::endl

            << "  --bootstrap                  "
            << "Write a cluster configuration into the very"
            << std::endl
            << "                               "
            << "first server's log and exit. This must only"
            << std::endl
            << "                               "
            << "be run once on a single server in each cluster."
            << std::endl

            << "  -c <file>, --config=<file>   "
            << "Set the path to the configuration file"
            << std::endl
            << "                               "
            << "[default: logcabin.conf]"
            << std::endl

            << "  -d, --daemon                 "
            << "Detach and run in the background"
            << std::endl
            << "                               "
            << "(requires --log)"
            << std::endl

            << "  -h, --help                   "
            << "Print this usage information"
            << std::endl

            << "  -l <file>, --log=<file>      "
            << "Write debug logs to <file> instead of stderr"
            << std::endl

            << "  -p <file>, --pidfile=<file>  "
            << "Write process ID to <file>"
            << std::endl

            << "  -t, --test                   "
            << "Check the configuration file for basic errors"
            << std::endl
            << "                               "
            << "and exit"
            << std::endl
            << std::endl

            << "Signals:"
            << std::endl

            << "  SIGUSR1                      "
            << "Dump ServerStats to debug log (experimental)"
            << std::endl

            << "  SIGUSR2                      "
            << "Reopen the debug log file"
            << std::endl;
    }

    int& argc;
    char**& argv;
    bool bootstrap;
    std::string configFilename;
    bool daemon;
    std::string debugLogFilename;
    std::string pidFilename;
    bool testConfig;
};

/**
 * RAII-style class to manage a file containing the process ID.
 */
class PidFile {
  public:
    explicit PidFile(const std::string& filename)
        : filename(filename)
        , written(-1)
    {
    }

    ~PidFile() {
        removeFile();
    }

    void writePid(int pid) {
        if (filename.empty())
            return;
        FILE* file = fopen(filename.c_str(), "w");
        if (file == NULL) {
            PANIC("Could not open %s for writing process ID: %s",
                  filename.c_str(),
                  strerror(errno));
        }
        std::string pidString =
            LogCabin::Core::StringUtil::format("%d\n", pid);
        size_t bytesWritten =
            fwrite(pidString.c_str(), 1, pidString.size(), file);
        if (bytesWritten != pidString.size()) {
            PANIC("Could not write process ID %s to pidfile %s: %s",
                  pidString.c_str(), filename.c_str(),
                  strerror(errno));
        }
        int r = fclose(file);
        if (r != 0) {
            PANIC("Could not close pidfile %s: %s",
                  filename.c_str(),
                  strerror(errno));
        }
        NOTICE("Wrote PID %d to %s",
               pid, filename.c_str());
        written = pid;
    }

    void removeFile() {
        if (written < 0)
            return;
        FILE* file = fopen(filename.c_str(), "r");
        if (file == NULL) {
            WARNING("Could not open %s for reading process ID prior to "
                    "removal: %s",
                    filename.c_str(),
                    strerror(errno));
            return;
        }
        char readbuf[10];
        memset(readbuf, 0, sizeof(readbuf));
        size_t bytesRead = fread(readbuf, 1, sizeof(readbuf), file);
        if (bytesRead == 0) {
            WARNING("PID could not be read from pidfile: "
                    "will not remove file %s",
                    filename.c_str());
            fclose(file);
            return;
        }
        int pidRead = atoi(readbuf);
        if (pidRead != written) {
            WARNING("PID read from pidfile (%d) does not match PID written "
                    "earlier (%d): will not remove file %s",
                    pidRead, written, filename.c_str());
            fclose(file);
            return;
        }
        int r = unlink(filename.c_str());
        if (r != 0) {
            WARNING("Could not unlink %s: %s",
                    filename.c_str(), strerror(errno));
            fclose(file);
            return;
        }
        written = -1;
        fclose(file);
        NOTICE("Removed pidfile %s", filename.c_str());
    }

    std::string filename;
    int written;
};

} // anonymous namespace

int
main(int argc, char** argv)
{
    using namespace LogCabin;

    try {
        Core::ThreadId::setName("evloop");

        // Parse command line args.
        OptionParser options(argc, argv);

        if (options.testConfig) {
            Server::Globals globals;
            globals.config.readFile(options.configFilename.c_str());
            // The following settings are required, and Config::read() throws
            // an exception with an OK error message if they aren't found:
            globals.config.read<uint64_t>("serverId");
            globals.config.read<std::string>("listenAddresses");
            return 0;
        }

        // Set debug log file
        if (!options.debugLogFilename.empty()) {
            std::string error =
                Core::Debug::setLogFilename(options.debugLogFilename);
            if (!error.empty()) {
                ERROR("Failed to set debug log file: %s",
                      error.c_str());
            }
        }

        NOTICE("Using config file %s", options.configFilename.c_str());

        // Detach as daemon
        if (options.daemon) {
            if (options.debugLogFilename.empty()) {
                ERROR("Refusing to run as daemon without a log file "
                      "(use /dev/null if you insist)");
            }
            NOTICE("Detaching");
            bool chdir = false; // leave the current working directory in case
                                // the user has specified relative paths for
                                // the config file, etc
            bool close = true;  // close stdin, stdout, stderr
            if (daemon(!chdir, !close) != 0) {
                PANIC("Call to daemon() failed: %s", strerror(errno));
            }
            int pid = getpid();
            Core::Debug::processName = Core::StringUtil::format("%d", pid);
            NOTICE("Detached as daemon with pid %d", pid);
        }

        // Write PID file, removed upon destruction
        PidFile pidFile(options.pidFilename);
        pidFile.writePid(getpid());

        {
            // Initialize and run Globals.
            Server::Globals globals;
            globals.config.readFile(options.configFilename.c_str());

            // Set debug log policy.
            // A few log messages above already got through; oh well.
            Core::Debug::setLogPolicy(
                Core::Debug::logPolicyFromString(
                    globals.config.read<std::string>("logPolicy", "NOTICE")));

            NOTICE("Config file settings:\n"
                   "# begin config\n"
                   "%s"
                   "# end config",
                   Core::StringUtil::toString(globals.config).c_str());
            globals.init();
            if (options.bootstrap) {
                globals.raft->bootstrapConfiguration();
                NOTICE("Done bootstrapping configuration. Exiting.");
            } else {
                globals.leaveSignalsBlocked();
                globals.run();
            }
        }

        google::protobuf::ShutdownProtobufLibrary();
        return 0;

    } catch (const Core::Config::Exception& e) {
        ERROR("Fatal exception from config file: %s",
              e.what());
    }
}
