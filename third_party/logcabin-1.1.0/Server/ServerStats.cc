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

#include <signal.h>

#include "Core/ProtoBuf.h"
#include "Core/ThreadId.h"
#include "Core/Time.h"
#include "Event/Signal.h"
#include "Server/Globals.h"
#include "Server/RaftConsensus.h"
#include "Server/StateMachine.h"
#include "Server/ServerStats.h"

namespace LogCabin {
namespace Server {

//// class ServerStats::Lock ////

ServerStats::Lock::Lock(ServerStats& wrapper)
    : wrapper(wrapper)
    , lockGuard(wrapper.mutex)
{
}

ServerStats::Lock::~Lock()
{
}

Protocol::ServerStats*
ServerStats::Lock::operator->()
{
    return &wrapper.stats;
}

Protocol::ServerStats&
ServerStats::Lock::operator*()
{
    return wrapper.stats;
}

//// class ServerStats::SignalHandler ////

ServerStats::SignalHandler::SignalHandler(ServerStats& serverStats)
    : Signal(SIGUSR1)
    , serverStats(serverStats)
{
}

void
ServerStats::SignalHandler::handleSignalEvent()
{
    NOTICE("Received %s: dumping ServerStats", strsignal(signalNumber));
    std::lock_guard<Core::Mutex> lockGuard(serverStats.mutex);
    serverStats.isStatsDumpRequested = true;
    serverStats.statsDumpRequested.notify_all();
}

//// class ServerStats::Deferred ////

ServerStats::Deferred::Deferred(ServerStats& serverStats)
    : signalHandler(serverStats)
    , signalMonitor(serverStats.globals.eventLoop, signalHandler)
    , dumpInterval(std::chrono::milliseconds(
        serverStats.globals.config.read<uint64_t>(
            "statsDumpIntervalMilliseconds", 60000)))
    , statsDumper()
{
    statsDumper = std::thread(&ServerStats::statsDumperMain, &serverStats);
}

//// class ServerStats ////

ServerStats::ServerStats(Globals& globals)
    : globals(globals)
    , mutex()
    , statsDumpRequested()
    , exiting(false)
    , isStatsDumpRequested(false)
    , lastDumped(SteadyClock::now())
    , stats()
    , deferred()
{
}

ServerStats::~ServerStats()
{
}

void
ServerStats::enable()
{
    Lock lock(*this);
    if (!deferred) {
        // Defer construction of timer so that TimerHandler constructor can
        // access globals.config.
        deferred.reset(new Deferred(*this));
    }
}

void
ServerStats::exit()
{
    {
        std::lock_guard<Core::Mutex> lockGuard(mutex);
        exiting = true;
        statsDumpRequested.notify_all();
    }
    if (deferred && deferred->statsDumper.joinable())
        deferred->statsDumper.join();
    deferred.reset();
}

void
ServerStats::dumpToDebugLog() const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    dumpToDebugLog(lockGuard);
}

Protocol::ServerStats
ServerStats::getCurrent() const
{
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    return getCurrent(lockGuard);
}

////////// ServerStats private //////////

void
ServerStats::dumpToDebugLog(std::unique_lock<Core::Mutex>& lockGuard) const
{
    isStatsDumpRequested = false;
    Protocol::ServerStats currentStats = getCurrent(lockGuard);
    NOTICE("ServerStats:\n%s",
           Core::ProtoBuf::dumpString(currentStats).c_str());
    SteadyClock::time_point now = SteadyClock::now();
    if (lastDumped < now)
        lastDumped = now;
}

Protocol::ServerStats
ServerStats::getCurrent(std::unique_lock<Core::Mutex>& lockGuard) const
{
    int64_t startTime = std::chrono::nanoseconds(
        Core::Time::SystemClock::now().time_since_epoch()).count();
    Protocol::ServerStats copy = stats;
    copy.set_start_at(startTime);
    if (deferred.get() != NULL) { // enabled
        // release lock to avoid deadlock and for concurrency
        Core::MutexUnlock<Core::Mutex> unlockGuard(lockGuard);
        globals.raft->updateServerStats(copy);
        globals.stateMachine->updateServerStats(copy);
    }
    copy.set_end_at(std::chrono::nanoseconds(
        Core::Time::SystemClock::now().time_since_epoch()).count());
    return copy;
}

void
ServerStats::statsDumperMain()
{
    typedef SteadyClock::time_point TimePoint;

    Core::ThreadId::setName("StatsDumper");
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    while (!exiting) {

        // Calculate time for next periodic dump.
        TimePoint nextDump = TimePoint::max();
        if (deferred->dumpInterval > std::chrono::nanoseconds::zero()) {
            nextDump = lastDumped + deferred->dumpInterval;
            if (nextDump < lastDumped) // overflow
                nextDump = TimePoint::max();
        }

        // Dump out stats to debug log
        if (isStatsDumpRequested || SteadyClock::now() >= nextDump) {
            dumpToDebugLog(lockGuard);
            continue;
        }

        // Wait until next time
        statsDumpRequested.wait_until(lockGuard, nextDump);
    }

    NOTICE("Shutting down");
}


} // namespace LogCabin::Server
} // namespace LogCabin
