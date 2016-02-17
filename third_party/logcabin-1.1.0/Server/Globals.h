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

#include <memory>

#include "Client/SessionManager.h"
#include "Core/Config.h"
#include "Core/Mutex.h"
#include "Event/Loop.h"
#include "Event/Signal.h"
#include "Server/ServerStats.h"

#ifndef LOGCABIN_SERVER_GLOBALS_H
#define LOGCABIN_SERVER_GLOBALS_H

namespace LogCabin {

// forward declarations
namespace RPC {
class Server;
}

namespace Server {

// forward declarations
class ClientService;
class ControlService;
class RaftConsensus;
class RaftService;
class StateMachine;

/**
 * Holds the LogCabin daemon's top-level objects.
 * The purpose of main() is to create and run a Globals object.
 * Other classes may refer to this object if they need access to other
 * top-level objects.
 */
class Globals {
  private:
    /**
     * Exits from the event loop upon receiving a UNIX signal.
     */
    class ExitHandler : public Event::Signal {
      public:
        ExitHandler(Event::Loop& eventLoop, int signalNumber);
        void handleSignalEvent();
        Event::Loop& eventLoop;
    };

    /**
     * Re-opens the log file upon receiving a UNIX signal.
     */
    class LogRotateHandler : public Event::Signal {
      public:
        LogRotateHandler(Event::Loop& eventLoop, int signalNumber);
        void handleSignalEvent();
        Event::Loop& eventLoop;
    };

  public:

    /// Constructor.
    Globals();

    /// Destructor.
    ~Globals();

    /**
     * Finish initializing this object.
     * This should be called after #config has been filled in.
     */
    void init();

    /**
     * Leave the signals blocked when this object is destroyed.
     * This is used in Server/Main.cc for the long-running daemon; it's not
     * used in unit tests.
     *
     * This was added to work around a specific problem: when running the
     * servers under valgrind through cluster.py, the servers would receive
     * SIGTERM, start to shut down, then the instant the SIGTERM signal was
     * unmasked, the server would appear to exit with a 0 status, yet it
     * wouldn't finish the shutdown process. I couldn't reproduce this outside
     * of cluster.py. As there's no reason to unblock the signals before
     * exiting the daemon, this seems like the safer bet for now.
     * -Diego 2015-04-29
     */
    void leaveSignalsBlocked();

    /**
     * Run the event loop until SIGINT, SIGTERM, or someone calls
     * Event::Loop::exit().
     */
    void run();

    /**
     * Enable asynchronous signal delivery for all signals that this class is
     * in charge of. This should be called in a child process after invoking
     * fork(), as the StateMachine does.
     */
    void unblockAllSignals();

    /**
     * Global configuration options.
     */
    Core::Config config;

    /**
     * Statistics and information about the server's current state. Useful for
     * diagnostics.
     */
    Server::ServerStats serverStats;

    /**
     * The event loop that runs the RPC system.
     */
    Event::Loop eventLoop;

  private:
    /**
     * Block SIGINT, which is handled by sigIntHandler.
     * Signals are blocked early on in the startup process so that newly
     * spawned threads also have them blocked.
     */
    Event::Signal::Blocker sigIntBlocker;

    /**
     * Block SIGTERM, which is handled by sigTermHandler.
     */
    Event::Signal::Blocker sigTermBlocker;

    /**
     * Block SIGUSR1, which is handled by serverStats.
     */
    Event::Signal::Blocker sigUsr1Blocker;

    /**
     * Block SIGUSR2, which is handled by sigUsr2Handler.
     */
    Event::Signal::Blocker sigUsr2Blocker;

    /**
     * Exits the event loop upon receiving SIGINT (keyboard interrupt).
     */
    ExitHandler sigIntHandler;

    /**
     * Registers sigIntHandler with the event loop.
     */
    Event::Signal::Monitor sigIntMonitor;

    /**
     * Exits the event loop upon receiving SIGTERM (kill).
     */
    ExitHandler sigTermHandler;

    /**
     * Registers sigTermHandler with the event loop.
     */
    Event::Signal::Monitor sigTermMonitor;

    /**
     * Re-opens log files upon receiving SIGUSR2 (user-defined signal). This
     * should normally be invoked by tools like logrotate.
     */
    LogRotateHandler sigUsr2Handler;

    /**
     * Registers sigUsr2Handler with the event loop.
     */
    Event::Signal::Monitor sigUsr2Monitor;

  public:
    /**
     * A unique ID for the cluster that this server may connect to. This is
     * initialized to a value from the config file. If it's not set then, it
     * may be set later as a result of learning a UUID from some other server.
     */
    Client::SessionManager::ClusterUUID clusterUUID;

    /**
     * Unique ID for this server. Set from config file.
     */
    uint64_t serverId;

    /**
     * Consensus module.
     */
    std::shared_ptr<Server::RaftConsensus> raft;

    /**
     * State machine used to process client requests.
     */
    std::shared_ptr<Server::StateMachine> stateMachine;

  private:

    /**
     * Service used by logcabinctl to query and change a server's internal
     * state.
     */
    std::shared_ptr<Server::ControlService> controlService;

    /**
     * Service used to communicate between servers.
     */
    std::shared_ptr<Server::RaftService> raftService;

    /**
     * The application-facing facing RPC service.
     */
    std::shared_ptr<Server::ClientService> clientService;

    /**
     * Listens for inbound RPCs and passes them off to the services.
     */
    std::unique_ptr<RPC::Server> rpcServer;

    // Globals is non-copyable.
    Globals(const Globals&) = delete;
    Globals& operator=(const Globals&) = delete;

}; // class Globals

} // namespace LogCabin::Server
} // namespace LogCabin

#endif /* LOGCABIN_SERVER_GLOBALS_H */
