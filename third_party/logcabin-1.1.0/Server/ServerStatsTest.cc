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

#include <gtest/gtest.h>

#include "Server/Globals.h"
#include "Server/ServerStats.h"

namespace LogCabin {
namespace Server {
namespace {

class ServerServerStatsTest : public ::testing::Test {
  public:
    ServerServerStatsTest()
      : globals()
    {
        globals.config.set("storageModule", "Memory");
        globals.config.set("uuid", "my-fake-uuid-123");
        globals.config.set("listenAddresses", "127.0.0.1");
        globals.config.set("use-temporary-storage", "true");
        globals.config.set("serverId", "1");
    }

    ~ServerServerStatsTest() {
    }

    void killStatsDumper()
    {
        {
            std::lock_guard<Core::Mutex> lockGuard(globals.serverStats.mutex);
            if (globals.serverStats.deferred.get() == NULL)
                return;
            globals.serverStats.exiting = true;
            globals.serverStats.statsDumpRequested.notify_all();
        }
        globals.serverStats.deferred->statsDumper.join();
        {
            std::lock_guard<Core::Mutex> lockGuard(globals.serverStats.mutex);
            globals.serverStats.exiting = false;
        }
    }


    Globals globals;
};

TEST_F(ServerServerStatsTest, Lock)
{
    ServerStats::Lock lockGuard(globals.serverStats);

    // This is a regression test for
    // https://github.com/logcabin/logcabin/issues/122
    EXPECT_FALSE(globals.serverStats.mutex.try_lock());

    EXPECT_FALSE(lockGuard->has_raft());
    EXPECT_FALSE((*lockGuard).has_raft());
}

TEST_F(ServerServerStatsTest, SignalHandler_handleSignalEvent)
{
    globals.init();
    killStatsDumper();
    EXPECT_FALSE(globals.serverStats.isStatsDumpRequested);
    EXPECT_EQ(1U, globals.serverStats.statsDumpRequested.notificationCount);
    globals.serverStats.deferred->signalHandler.handleSignalEvent();
    EXPECT_TRUE(globals.serverStats.isStatsDumpRequested);
    EXPECT_EQ(2U, globals.serverStats.statsDumpRequested.notificationCount);
}

TEST_F(ServerServerStatsTest, SignalHandler_handleSignalEvent_TimingSensitive)
{
    globals.init();
    std::thread eventLoop(&Globals::run, &globals);
    EXPECT_EQ(0U, globals.serverStats.statsDumpRequested.notificationCount);
    EXPECT_EQ(0, kill(getpid(), SIGUSR1));
    Core::Time::sleep(std::chrono::milliseconds(2));
    globals.eventLoop.exit();
    eventLoop.join();
    EXPECT_EQ(1U, globals.serverStats.statsDumpRequested.notificationCount);
}

TEST_F(ServerServerStatsTest, enable)
{
    globals.init();
    globals.serverStats.enable();
    EXPECT_TRUE(globals.serverStats.deferred.get() != NULL);
}

TEST_F(ServerServerStatsTest, exit)
{
    globals.init();
    globals.serverStats.enable();
    globals.serverStats.exit();
    globals.serverStats.exit();
}

TEST_F(ServerServerStatsTest, dumpToDebugLog)
{
    killStatsDumper();
    globals.serverStats.isStatsDumpRequested = true;
    auto now = ServerStats::SteadyClock::now();
    globals.serverStats.lastDumped = now;
    globals.serverStats.dumpToDebugLog();
    EXPECT_FALSE(globals.serverStats.isStatsDumpRequested);
    EXPECT_LT(now, globals.serverStats.lastDumped);
}

TEST_F(ServerServerStatsTest, getCurrent)
{
    Protocol::ServerStats stat = globals.serverStats.getCurrent();
    EXPECT_LT(stat.start_at(),
              stat.end_at());
    EXPECT_FALSE(stat.has_raft());
    EXPECT_TRUE(NULL == globals.serverStats.deferred.get());
    globals.init(); // calls globals.ServerStats.enable()
    EXPECT_TRUE(NULL != globals.serverStats.deferred.get());
    stat = globals.serverStats.getCurrent();
    EXPECT_TRUE(stat.has_raft());
}


struct StatsDumperMainHelper {
    explicit StatsDumperMainHelper(ServerStats& serverStats)
        : serverStats(serverStats)
        , count(0)
        , then()
    {
    }

    std::chrono::nanoseconds
    waitedFor()
    {
        return (serverStats.statsDumpRequested.lastWaitUntil -
                Core::Time::SteadyClock::now());
    }

    void operator()() {
        if (count == 0) {
            // dumpInterval = 0, !isStatsDumpRequested
            EXPECT_EQ(std::chrono::nanoseconds::zero(),
                      serverStats.deferred->dumpInterval);
            EXPECT_FALSE(serverStats.isStatsDumpRequested);

            EXPECT_LT(std::chrono::minutes(10), waitedFor());
            EXPECT_EQ(ServerStats::SteadyClock::time_point::min(),
                      serverStats.lastDumped);

            then = ServerStats::SteadyClock::now();
            serverStats.lastDumped = then;
            serverStats.deferred->dumpInterval = std::chrono::minutes(2);
        } else if (count == 1) {
            // dumpInterval = 120, not expired, !isStatsDumpRequested
            EXPECT_LT(std::chrono::minutes(1), waitedFor());
            EXPECT_GT(std::chrono::minutes(3), waitedFor());
            EXPECT_EQ(then, serverStats.lastDumped);

            then = ServerStats::SteadyClock::now();
            serverStats.lastDumped = then - std::chrono::minutes(3);
        } else if (count == 2) {
            // dumpInterval = 120, expired, !isStatsDumpRequested
            EXPECT_LT(std::chrono::minutes(1), waitedFor());
            EXPECT_GT(std::chrono::minutes(3), waitedFor());
            EXPECT_LT(then, serverStats.lastDumped);

            then = ServerStats::SteadyClock::now();
            serverStats.isStatsDumpRequested = true;
        } else if (count == 3) {
            // dumpInterval = 120, not expired, isStatsDumpRequested
            EXPECT_LT(std::chrono::minutes(1), waitedFor());
            EXPECT_GT(std::chrono::minutes(3), waitedFor());
            EXPECT_LT(then, serverStats.lastDumped);
            EXPECT_FALSE(serverStats.isStatsDumpRequested);

            serverStats.exiting = true;
        }
        ++count;

    }
    ServerStats& serverStats;
    uint64_t count;
    ServerStats::SteadyClock::time_point then;
};

TEST_F(ServerServerStatsTest, statsDumperMain)
{
    StatsDumperMainHelper helper(globals.serverStats);
    globals.serverStats.statsDumpRequested.callback = std::ref(helper);
    globals.config.set("statsDumpIntervalMilliseconds", "0");
    globals.init();
    killStatsDumper();
    globals.serverStats.lastDumped =
        ServerStats::SteadyClock::time_point::min();

    globals.serverStats.statsDumperMain();
    EXPECT_EQ(4U, helper.count);
}

} // namespace LogCabin::Server::<anonymous>
} // namespace LogCabin::Server
} // namespace LogCabin
