/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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
#include <sys/file.h>
#include <unistd.h>

#include <functional>
#include <iostream>
#include <string>

#include "build/Server/SnapshotMetadata.pb.h"
#include "build/Server/SnapshotStateMachine.pb.h"
#include "Core/Config.h"
#include "Core/Debug.h"
#include "Core/ProtoBuf.h"
#include "Core/StringUtil.h"
#include "Core/ThreadId.h"
#include "Core/Util.h"
#include "Storage/Layout.h"
#include "Storage/Log.h"
#include "Storage/LogFactory.h"
#include "Storage/SnapshotFile.h"
#include "Tree/Tree.h"

namespace {

using namespace LogCabin;

/**
 * Parses argv for the main function.
 */
class OptionParser {
  public:
    OptionParser(int& argc, char**& argv)
        : argc(argc)
        , argv(argv)
        , configFilename("logcabin.conf")
    {
        while (true) {
            static struct option longOptions[] = {
               {"config",  required_argument, NULL, 'c'},
               {"help",  no_argument, NULL, 'h'},
               {0, 0, 0, 0}
            };
            int c = getopt_long(argc, argv, "c:hi:", longOptions, NULL);

            // Detect the end of the options.
            if (c == -1)
                break;

            switch (c) {
                case 'h':
                    usage();
                    exit(0);
                case 'c':
                    configFilename = optarg;
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
            << "Dumps out the contents of LogCabin's storage directory "
            << "(the log and snapshot)."
            << std::endl
            << "This will refuse to run while LogCabin is running, since "
            << "it does the"
            << std::endl
            << "equivalent of a fsck for the log."
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

            << "  -h, --help                   "
            << "Print this usage information"
            << std::endl

            << "  -c <file>, --config=<file>   "
            << "Set the path to the configuration file"
            << std::endl
            << "                               "
            << "[default: logcabin.conf]"
            << std::endl;
    }

    int& argc;
    char**& argv;
    std::string configFilename;
};

void
dumpTree(const Tree::Tree& tree, const std::string& path = "/")
{
    std::cout << path << std::endl;
    std::vector<std::string> children;
    tree.listDirectory(path, children);
    for (auto it = children.begin();
         it != children.end();
         ++it) {
        if (Core::StringUtil::endsWith(*it, "/")) {
            dumpTree(tree, path + *it);
        } else {
            std::string contents;
            tree.read(path + *it, contents);
            std::cout << path << *it << " : " << contents << std::endl;
        }
    }
}

void
readSnapshot(Storage::Layout& storageLayout)
{
    std::unique_ptr<Storage::SnapshotFile::Reader> reader;
    try {
        reader.reset(new Storage::SnapshotFile::Reader(storageLayout));
    } catch (const std::runtime_error& e) { // file not found
        NOTICE("%s", e.what());
        return;
    }

    { // Check that this snapshot uses format version 1
        uint8_t version = 0;
        uint64_t bytesRead = reader->readRaw(&version, sizeof(version));
        if (bytesRead < 1) {
            PANIC("Found completely empty snapshot file (it doesn't even "
                  "have a version field)");
        } else {
            if (version != 1) {
                PANIC("Snapshot format version read was %u, but this code "
                      "can only read version 1",
                      version);
            }
        }
    }

    { // read header protobuf from stream
        Server::SnapshotMetadata::Header header;
        std::string error = reader->readMessage(header);
        if (!error.empty()) {
            PANIC("couldn't read snapshot header: %s",
                  error.c_str());
        }
        NOTICE("Snapshot header start");
        std::cout << Core::ProtoBuf::dumpString(header) << std::endl;
        NOTICE("Snapshot header end");
    }

    { // Check that the state machine part of the snapshot uses format
      // version 1
        uint8_t version = 0;
        uint64_t bytesRead = reader->readRaw(&version, sizeof(version));
        if (bytesRead < 1) {
            PANIC("Snapshot file too short (no state machine version "
                  "field)");
        } else {
            if (version != 1) {
                PANIC("State machine format version in snapshot read was "
                      "%u, but this code can only read version 1",
                      version);
            }
        }
    }

    { // Load snapshot header
        Server::SnapshotStateMachine::Header header;
        std::string error = reader->readMessage(header);
        if (!error.empty()) {
            PANIC("Couldn't read state machine header from snapshot: %s",
                  error.c_str());
        }
        NOTICE("Snapshot state machine header start");
        std::cout << Core::ProtoBuf::dumpString(header) << std::endl;
        NOTICE("Snapshot state machine header end");

    }

    { // read Tree from stream
        Tree::Tree tree;
        tree.loadSnapshot(*reader);
        NOTICE("Snapshot tree start");
        dumpTree(tree);
        NOTICE("Snapshot tree end");
    }
}

} // anonymous namespace

int
main(int argc, char** argv)
{
    using namespace LogCabin;

    try {

        Core::Util::Finally _(google::protobuf::ShutdownProtobufLibrary);
        Core::ThreadId::setName("main");

        // Parse command line args.
        OptionParser options(argc, argv);

        NOTICE("Using config file %s", options.configFilename.c_str());
        Core::Config config;
        config.readFile(options.configFilename.c_str());

        Core::Debug::setLogPolicy(
            Core::Debug::logPolicyFromString(
                config.read<std::string>("logPolicy", "NOTICE")));

        uint64_t serverId = config.read<uint64_t>("serverId");
        NOTICE("Server ID is %lu", serverId);

        Storage::Layout storageLayout;
        storageLayout.init(config, serverId);

        NOTICE("Opening log at %s", storageLayout.serverDir.path.c_str());
        {
            std::unique_ptr<Storage::Log> log =
                Storage::LogFactory::makeLog(config, storageLayout);
            NOTICE("Log contents start");
            std::cout << *log << std::endl;
            NOTICE("Log contents end");
        }

        NOTICE("Reading snapshot at %s", storageLayout.serverDir.path.c_str());
        readSnapshot(storageLayout);

        return 0;

    } catch (const Core::Config::Exception& e) {
        ERROR("Fatal exception from config file: %s",
              e.what());
    }
}
