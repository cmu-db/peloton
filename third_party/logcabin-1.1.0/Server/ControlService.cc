/* Copyright (c) 2015 Diego Ongaro
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

#include <unistd.h>

#include "build/Protocol/ServerControl.pb.h"
#include "Core/Debug.h"
#include "RPC/ServerRPC.h"
#include "Server/ControlService.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/StateMachine.h"

namespace LogCabin {
namespace Server {

ControlService::ControlService(Globals& globals)
    : globals(globals)
{
}

ControlService::~ControlService()
{
}

void
ControlService::handleRPC(RPC::ServerRPC rpc)
{
    using Protocol::ServerControl::OpCode;

    // Call the appropriate RPC handler based on the request's opCode.
    switch (rpc.getOpCode()) {
        case OpCode::DEBUG_FILENAME_GET:
            debugFilenameGet(std::move(rpc));
            break;
        case OpCode::DEBUG_FILENAME_SET:
            debugFilenameSet(std::move(rpc));
            break;
        case OpCode::DEBUG_POLICY_GET:
            debugPolicyGet(std::move(rpc));
            break;
        case OpCode::DEBUG_POLICY_SET:
            debugPolicySet(std::move(rpc));
            break;
        case OpCode::DEBUG_ROTATE:
            debugRotate(std::move(rpc));
            break;
        case OpCode::SERVER_INFO_GET:
            serverInfoGet(std::move(rpc));
            break;
        case OpCode::SERVER_STATS_DUMP:
            serverStatsDump(std::move(rpc));
            break;
        case OpCode::SERVER_STATS_GET:
            serverStatsGet(std::move(rpc));
            break;
        case OpCode::SNAPSHOT_CONTROL:
            snapshotControl(std::move(rpc));
            break;
        case OpCode::SNAPSHOT_INHIBIT_GET:
            snapshotInhibitGet(std::move(rpc));
            break;
        case OpCode::SNAPSHOT_INHIBIT_SET:
            snapshotInhibitSet(std::move(rpc));
            break;
        default:
            WARNING("Client sent request with bad op code (%u) to "
                    "ControlService", rpc.getOpCode());
            rpc.rejectInvalidRequest();
    }
}

std::string
ControlService::getName() const
{
    return "ControlService";
}

/**
 * Place this at the top of each RPC handler. Afterwards, 'request' will refer
 * to the protocol buffer for the request with all required fields set.
 * 'response' will be an empty protocol buffer for you to fill in the response.
 */
#define PRELUDE(rpcClass) \
    Protocol::ServerControl::rpcClass::Request request; \
    Protocol::ServerControl::rpcClass::Response response; \
    if (!rpc.getRequest(request)) \
        return;

////////// RPC handlers //////////

void
ControlService::debugFilenameGet(RPC::ServerRPC rpc)
{
    PRELUDE(DebugFilenameGet);
    response.set_filename(Core::Debug::getLogFilename());
    rpc.reply(response);
}

void
ControlService::debugFilenameSet(RPC::ServerRPC rpc)
{
    PRELUDE(DebugFilenameSet);
    std::string prev = Core::Debug::getLogFilename();
    NOTICE("Switching to log file %s",
           request.filename().c_str());
    std::string error = Core::Debug::setLogFilename(request.filename());
    if (error.empty()) {
        NOTICE("Switched from log file %s",
               prev.c_str());
    } else {
        ERROR("Failed to switch to log file %s: %s",
              request.filename().c_str(),
              error.c_str());
        response.set_error(error);
    }
    rpc.reply(response);
}

void
ControlService::debugPolicyGet(RPC::ServerRPC rpc)
{
    PRELUDE(DebugPolicyGet);
    response.set_policy(
            Core::Debug::logPolicyToString(
                Core::Debug::getLogPolicy()));
    rpc.reply(response);
}

void
ControlService::debugPolicySet(RPC::ServerRPC rpc)
{
    PRELUDE(DebugPolicySet);
    NOTICE("Switching to log policy %s",
           request.policy().c_str());
    Core::Debug::setLogPolicy(
            Core::Debug::logPolicyFromString(
                request.policy()));
    rpc.reply(response);
}

void
ControlService::debugRotate(RPC::ServerRPC rpc)
{
    PRELUDE(DebugRotate);
    NOTICE("Rotating logs");
    std::string error = Core::Debug::reopenLogFromFilename();
    if (error.empty()) {
        NOTICE("Done rotating logs");
    } else {
        ERROR("Failed to rotate log file: %s",
              error.c_str());
        response.set_error(error);
    }
    rpc.reply(response);
}

void
ControlService::serverInfoGet(RPC::ServerRPC rpc)
{
    PRELUDE(ServerInfoGet);
    response.set_server_id(globals.raft->serverId);
    response.set_addresses(globals.raft->serverAddresses);
    response.set_process_id(uint64_t(getpid()));
    rpc.reply(response);
}

void
ControlService::serverStatsDump(RPC::ServerRPC rpc)
{
    PRELUDE(ServerStatsDump);
    NOTICE("Requested dump of ServerStats through ServerControl RPC");
    globals.serverStats.dumpToDebugLog();
    rpc.reply(response);
}

void
ControlService::serverStatsGet(RPC::ServerRPC rpc)
{
    PRELUDE(ServerStatsGet);
    *response.mutable_server_stats() = globals.serverStats.getCurrent();
    rpc.reply(response);
}

void
ControlService::snapshotControl(RPC::ServerRPC rpc)
{
    PRELUDE(SnapshotControl);
    using Protocol::ServerControl::SnapshotCommand;
    switch (request.command()) {
        case SnapshotCommand::START_SNAPSHOT:
            globals.stateMachine->startTakingSnapshot();
            break;
        case SnapshotCommand::STOP_SNAPSHOT:
            globals.stateMachine->stopTakingSnapshot();
            break;
        case SnapshotCommand::RESTART_SNAPSHOT:
            globals.stateMachine->stopTakingSnapshot();
            globals.stateMachine->startTakingSnapshot();
            break;
        case SnapshotCommand::UNKNOWN_SNAPSHOT_COMMAND: // fallthrough
        default:
            response.set_error("Unknown SnapshotControl command");
    }
    rpc.reply(response);
}

void
ControlService::snapshotInhibitGet(RPC::ServerRPC rpc)
{
    PRELUDE(SnapshotInhibitGet);
    std::chrono::nanoseconds duration = globals.stateMachine->getInhibit();
    assert(duration >= std::chrono::nanoseconds::zero());
    response.set_nanoseconds(uint64_t(duration.count()));
    rpc.reply(response);
}

void
ControlService::snapshotInhibitSet(RPC::ServerRPC rpc)
{
    PRELUDE(SnapshotInhibitSet);
    bool abort = false;
    std::chrono::nanoseconds duration;
    if (request.has_nanoseconds()) {
        duration = std::chrono::nanoseconds(request.nanoseconds());
        if (request.nanoseconds() > 0 &&
            duration < std::chrono::nanoseconds::zero()) { // overflow
            duration = std::chrono::nanoseconds::max();
        }
        if (request.nanoseconds() == 0)
            abort = false;
    } else {
        duration = std::chrono::nanoseconds::max();
    }
    globals.stateMachine->setInhibit(duration);
    if (abort) {
        globals.stateMachine->stopTakingSnapshot();
    }
    rpc.reply(response);
}


} // namespace LogCabin::Server
} // namespace LogCabin
