/* Copyright (c) 2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
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

#include <signal.h>

#include "Core/Debug.h"
#include "Core/StringUtil.h"
#include "Protocol/Common.h"
#include "RPC/Server.h"
#include "Server/ClientService.h"
#include "Server/ControlService.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/RaftService.h"
#include "Server/StateMachine.h"

namespace LogCabin {
namespace Server {

////////// Globals::SigIntHandler //////////

Globals::ExitHandler::ExitHandler(
        Event::Loop& eventLoop,
        int signalNumber)
    : Signal(signalNumber)
    , eventLoop(eventLoop)
{
}

void
Globals::ExitHandler::handleSignalEvent()
{
    NOTICE("%s: shutting down", strsignal(signalNumber));
    eventLoop.exit();
}

Globals::LogRotateHandler::LogRotateHandler(
        Event::Loop& eventLoop,
        int signalNumber)
    : Signal(signalNumber)
    , eventLoop(eventLoop)
{
}

void
Globals::LogRotateHandler::handleSignalEvent()
{
    NOTICE("%s: rotating logs", strsignal(signalNumber));
    std::string error = Core::Debug::reopenLogFromFilename();
    if (!error.empty()) {
        PANIC("Failed to rotate log file: %s",
              error.c_str());
    }
    NOTICE("%s: done rotating logs", strsignal(signalNumber));
}


////////// Globals //////////

Globals::Globals()
    : config()
    , serverStats(*this)
    , eventLoop()
    , sigIntBlocker(SIGINT)
    , sigTermBlocker(SIGTERM)
    , sigUsr1Blocker(SIGUSR1)
    , sigUsr2Blocker(SIGUSR2)
    , sigIntHandler(eventLoop, SIGINT)
    , sigIntMonitor(eventLoop, sigIntHandler)
    , sigTermHandler(eventLoop, SIGTERM)
    , sigTermMonitor(eventLoop, sigTermHandler)
    , sigUsr2Handler(eventLoop, SIGUSR2)
    , sigUsr2Monitor(eventLoop, sigUsr2Handler)
    , clusterUUID()
    , serverId(~0UL)
    , raft()
    , stateMachine()
    , controlService()
    , raftService()
    , clientService()
    , rpcServer()
{
}

Globals::~Globals()
{
    serverStats.exit();
}

void
Globals::init()
{
    std::string uuid = config.read("clusterUUID", std::string(""));
    if (!uuid.empty())
        clusterUUID.set(uuid);
    serverId = config.read<uint64_t>("serverId");
    Core::Debug::processName = Core::StringUtil::format("%lu", serverId);
    {
        ServerStats::Lock serverStatsLock(serverStats);
        serverStatsLock->set_server_id(serverId);
    }
    if (!raft) {
        raft.reset(new RaftConsensus(*this));
        raft->serverId = serverId;
    }

    if (!controlService) {
        controlService.reset(new ControlService(*this));
    }

    if (!raftService) {
        raftService.reset(new RaftService(*this));
    }

    if (!clientService) {
        clientService.reset(new ClientService(*this));
    }

    if (!rpcServer) {
        rpcServer.reset(new RPC::Server(eventLoop,
                                        Protocol::Common::MAX_MESSAGE_LENGTH));

        uint32_t maxThreads = config.read<uint16_t>("maxThreads", 16);
        namespace ServiceId = Protocol::Common::ServiceId;
        rpcServer->registerService(ServiceId::CONTROL_SERVICE,
                                   controlService,
                                   maxThreads);
        rpcServer->registerService(ServiceId::RAFT_SERVICE,
                                   raftService,
                                   maxThreads);
        rpcServer->registerService(ServiceId::CLIENT_SERVICE,
                                   clientService,
                                   maxThreads);

        std::string listenAddressesStr =
            config.read<std::string>("listenAddresses");
        {
            ServerStats::Lock serverStatsLock(serverStats);
            serverStatsLock->set_server_id(serverId);
            serverStatsLock->set_addresses(listenAddressesStr);
        }
        std::vector<std::string> listenAddresses =
            Core::StringUtil::split(listenAddressesStr, ',');
        if (listenAddresses.empty()) {
            EXIT("No server addresses specified to listen on");
        }
        for (auto it = listenAddresses.begin();
             it != listenAddresses.end();
             ++it) {
            RPC::Address address(*it, Protocol::Common::DEFAULT_PORT);
            address.refresh(RPC::Address::TimePoint::max());
            std::string error = rpcServer->bind(address);
            if (!error.empty()) {
                EXIT("Could not listen on address %s: %s",
                     address.toString().c_str(),
                     error.c_str());
            }
            NOTICE("Serving on %s",
                   address.toString().c_str());
        }
        raft->serverAddresses = listenAddressesStr;
        raft->init();
    }

    if (!stateMachine) {
        stateMachine.reset(new StateMachine(raft, config, *this));
    }

    serverStats.enable();
}

void
Globals::leaveSignalsBlocked()
{
    sigIntBlocker.leaveBlocked();
    sigTermBlocker.leaveBlocked();
    sigUsr1Blocker.leaveBlocked();
    sigUsr2Blocker.leaveBlocked();
}

void
Globals::run()
{
    eventLoop.runForever();
}

void
Globals::unblockAllSignals()
{
    sigIntBlocker.unblock();
    sigTermBlocker.unblock();
    sigUsr1Blocker.unblock();
    sigUsr2Blocker.unblock();
}


} // namespace LogCabin::Server
} // namespace LogCabin
