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

int
logcabin_main(char* arg)
{
    using namespace LogCabin;
    bool bootstrap = false;
    char *serverID = NULL;
    char *connString = NULL;
    int argc = 0;

    std::istringstream argument_stream(arg);

    for (;;)
    {
        std::string token;

        if (!(argument_stream >> token)) 
            break;

        if (strcmp("bootstrap",token.c_str()) == 0)
        {
            bootstrap = true;
            continue;
        }

        switch (argc)
        {
            case 0: 
                    serverID = strdup(token.c_str());
                    break;
            case 1:
                    connString = strdup(token.c_str());
                    break;
        }
            argc++;
    }

    try {
        Core::ThreadId::setName("evloop");

        {
        // Initialize and run Globals.
        Server::Globals globals;
        globals.config.set("serverId",serverID);
        globals.config.set("listenAddresses", connString);

        Core::Debug::setLogPolicy(
            Core::Debug::logPolicyFromString(
                globals.config.read<std::string>("logPolicy", "NOTICE")));

        globals.init();
        if (bootstrap) 
        {
            globals.raft->bootstrapConfiguration();
        } 
        else 
        {
            globals.leaveSignalsBlocked();
            globals.run();
        }
        }

        google::protobuf::ShutdownProtobufLibrary();
        return 0;

    } catch (const Core::Config::Exception& e) {
        ERROR("Fatal exception %s",
              e.what());
        return -1;
    }

}
