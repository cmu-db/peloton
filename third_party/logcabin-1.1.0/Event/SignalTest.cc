/* Copyright (c) 2012 Stanford University
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
#include <signal.h>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Event/Signal.h"

namespace LogCabin {
namespace Event {
namespace {

struct ExitOnSignal : public Event::Signal {
    ExitOnSignal(Event::Loop& loop, int signal)
        : Signal(signal)
        , eventLoop(loop)
        , triggerCount(0)
    {
    }
    void handleSignalEvent() {
        ++triggerCount;
        eventLoop.exit();
    }
    Event::Loop& eventLoop;
    uint32_t triggerCount;
};

struct ExitOnTimer : public Event::Timer {
    explicit ExitOnTimer(Event::Loop& loop)
        : Timer()
        , eventLoop(loop)
        , triggerCount(0)
    {
    }
    void handleTimerEvent() {
        ++triggerCount;
        eventLoop.exit();
    }
    Event::Loop& eventLoop;
    uint32_t triggerCount;
};

struct EventSignalTest : public ::testing::Test {
    EventSignalTest()
        : loop()
    {
    }
    Event::Loop loop;
};

struct EventSignalBlockerTest : EventSignalTest {
};

TEST_F(EventSignalBlockerTest, constructor) {
    Event::Signal::Blocker block(SIGTERM);
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
    EXPECT_EQ(0, kill(getpid(), SIGTERM));

    ExitOnTimer timer(loop);
    Event::Timer::Monitor timerMonitor(loop, timer);
    timer.schedule(1000*1000);
    loop.runForever();
    EXPECT_EQ(1U, signal.triggerCount);
}

TEST_F(EventSignalBlockerTest, destructor) {
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
    {
        Event::Signal::Blocker block(SIGTERM);
    }
    EXPECT_DEATH(kill(getpid(), SIGTERM),
                 "");
}

TEST_F(EventSignalBlockerTest, block) {
    Event::Signal::Blocker block(SIGTERM);
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
    block.unblock();
    block.block();
    block.block();
    EXPECT_EQ(0, kill(getpid(), SIGTERM));

    ExitOnTimer timer(loop);
    Event::Timer::Monitor timerMonitor(loop, timer);
    timer.schedule(1000*1000);
    loop.runForever();
    EXPECT_EQ(1U, signal.triggerCount);
}

TEST_F(EventSignalBlockerTest, unblock) {
    Event::Signal::Blocker block(SIGTERM);
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
    block.unblock();
    block.unblock();
    EXPECT_DEATH(kill(getpid(), SIGTERM),
                 "");
}

TEST_F(EventSignalTest, constructor) {
    Event::Signal::Blocker block(SIGTERM);
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
}

TEST_F(EventSignalTest, destructor) {
    // Nothing to test.
}

TEST_F(EventSignalTest, fires) {
    Event::Signal::Blocker block(SIGTERM);
    ExitOnSignal signal(loop, SIGTERM);
    Event::Signal::Monitor monitor(loop, signal);
    // Warning: if you run this in gdb, you'll need to pass the signal through
    // to the application.
    EXPECT_EQ(0, kill(getpid(), SIGTERM));
    // must have been caught if we get this far
    loop.runForever();
    EXPECT_EQ(1U, signal.triggerCount);
}

} // namespace LogCabin::Event::<anonymous>
} // namespace LogCabin::Event
} // namespace LogCabin
