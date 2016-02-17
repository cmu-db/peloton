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

#ifndef LOGCABIN_SERVER_SERVERSTATS_H
#define LOGCABIN_SERVER_SERVERSTATS_H

#include <memory>
#include <thread>

#include "build/Protocol/ServerStats.pb.h"

#include "Core/Mutex.h"
#include "Core/Time.h"
#include "Event/Signal.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Server {

// forward declaration
class Globals;

/**
 * Manages creation of server statistics, which are used for diagnostics.
 *
 * Server statistics are gathered in two ways. First, this object maintains a
 * #stats structure that modules can fill in by acquiring a Lock and modifying
 * directly. This #stats structure is copied every time stats are requested.
 * Second, when stats are requested, #getCurrent() will ask certain modules
 * (such as RaftConsensus) to fill in the current information into a copy of
 * the stats structure.
 */
class ServerStats {
  public:
    /// Constructor.
    explicit ServerStats(Globals& globals);
    /// Destructor.
    ~ServerStats();

    /**
     * Called after Globals are initialized to finish setting up this class.
     * Attaches signal handler and starts stats dumper thread. Starts calling
     * other modules for their state in getCurrent().
     */
    void enable();

    /**
     * Prepare for shutdown. Waits for the statsDumper thread to exit, and
     * destroys the #deferred object (the opposite of #enable()).
     */
    void exit();

    /**
     * Write the current stats to the debug log (NOTICE level).
     * This is preferred over calling getCurrent() followed by NOTICE(), since
     * it will arrange for the next periodic stats dump to be delayed (there's
     * not much sense in a periodic stats dump immediately following a manual
     * stats dump).
     */
    void dumpToDebugLog() const;

    /**
     * Calculate and return the current server stats.
     */
    Protocol::ServerStats getCurrent() const;

    /**
     * Provides read/write access to #stats, protected against concurrent
     * access.
     */
    class Lock {
      public:
        /// Constructor.
        explicit Lock(ServerStats& wrapper);
        /// Destructor.
        ~Lock();
        /// Structure dereference operator. Returns stats pointer.
        Protocol::ServerStats* operator->();
        /// Indirection (dereference) operator. Returns stats reference.
        Protocol::ServerStats& operator*();
      private:
        /// Handle to containing class.
        ServerStats& wrapper;
        /// Locks #mutex for the lifetime of this object.
        std::lock_guard<Core::Mutex> lockGuard;
    };

  private:
    /**
     * Used to dump stats periodically.
     */
    typedef Core::Time::SteadyClock SteadyClock;

    /**
     * Used to include wall time in stats.
     */
    typedef Core::Time::SystemClock SystemClock;

    /**
     * Asks statsDumper thread to dumps stats to the debug log (NOTICE level)
     * on SIGUSR1 signal.
     * (We don't ever want to collect stats from the Event Loop thread, since
     * that might stall the event loop for too long and/or acquire mutexes in
     * incorrect orders, opening up the possibility for deadlock.)
     */
    class SignalHandler : public Event::Signal {
      public:
        /// Constructor. Registers itself as SIGUSR1 handler.
        explicit SignalHandler(ServerStats& serverStats);
        /// Fires when SIGUSR1 is received. Prints the stats to the log.
        void handleSignalEvent();
        /// Handle to containing class.
        ServerStats& serverStats;
    };

    /**
     * Members that are constructed later, during enable().
     * Whereas the ServerStats is constructed early in the server startup
     * process, these members get to access globals and globals.config in their
     * constructors.
     */
    struct Deferred {
        /**
         * Constructor. Called during enable().
         */
        explicit Deferred(ServerStats& serverStats);

        /**
         * See SignalHandler.
         */
        SignalHandler signalHandler;

        /**
         * Registers signalHandler with event loop.
         */
        Event::Signal::Monitor signalMonitor;

        /**
         * If nonzero, the #statsDumper thread will write the current stats to
         * the debug log if this duration has elapsed since the last dump.
         */
        std::chrono::nanoseconds dumpInterval;

        /**
         * This thread dumps the stats periodically and when signalled.
         * Dumping the stats is done from a separate thread so that the event
         * loop thread does not acquire locks in higher-level modules (which
         * could result in deadlocks or delays). Introduced for
         * https://github.com/logcabin/logcabin/issues/159 .
         */
        std::thread statsDumper;
    };

    /**
     * See public #dumpToDebugLog() above.
     * Internally releases and reacquires the lock for concurrency and to avoid
     * deadlock.
     */
    void dumpToDebugLog(std::unique_lock<Core::Mutex>& lockGuard) const;

    /**
     * See public #getCurrent() above.
     * Internally releases and reacquires the lock for concurrency and to avoid
     * deadlock.
     */
    Protocol::ServerStats
    getCurrent(std::unique_lock<Core::Mutex>& lockGuard) const;

    void statsDumperMain();

    /**
     * Server-wide objects.
     */
    Globals& globals;

    /**
     * Protects all of the following members of this class.
     */
    mutable Core::Mutex mutex;

    /**
     * Notified when #isStatsDumpRequested is set and when #exiting is set.
     * statsDumper waits on this.
     */
    Core::ConditionVariable statsDumpRequested;

    /**
     * Set to true when statsDumper should exit.
     */
    bool exiting;

    /**
     * Set to true when statsDumper should write the entire stats to the debug
     * log. Mutable so that it can be reset to false in const methods such as
     * dumpToDebugLog().
     */
    mutable bool isStatsDumpRequested;

    /**
     * The last time when the stats were written to the debug log.
     * Monotonically increases.
     */
    mutable Core::Time::SteadyClock::time_point lastDumped;

    /**
     * Partially filled-in structure that is copied as the basis of all calls
     * to getCurrent().
     */
    Protocol::ServerStats stats;

    /**
     * See Deferred. If non-NULL, enabled() has already been called, and other
     * modules should be queried for stats during getCurrent().
     */
    std::unique_ptr<Deferred> deferred;
};

} // namespace LogCabin::Server
} // namespace LogCabin

#endif /* LOGCABIN_SERVER_SERVERSTATS_H */
