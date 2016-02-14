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
#include <thread>

#include "Event/Loop.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Event {
namespace {

class Counter : public Event::Timer {
    explicit Counter(Event::Loop& eventLoop)
        : Timer()
        , eventLoop(eventLoop)
        , count(0)
        , exitAfter(0)
    {
    }
    void handleTimerEvent() {
        ++count;
        if (count > exitAfter)
            eventLoop.exit();
        else
            schedule(0);
    }
    Event::Loop& eventLoop;
    uint64_t count;
    uint64_t exitAfter;
};

void
lockAndIncrement100(Event::Loop& eventLoop, uint64_t& count)
{
    for (uint32_t i = 0; i < 100; ++i) {
        Loop::Lock lock(eventLoop);
        uint64_t start = count;
        usleep(5);
        count = start + 1;
    }
}

TEST(EventLoopTest, lock) {
    Loop loop;
    Counter counter(loop);
    Timer::Monitor counterMonitor(loop, counter);
    counter.schedule(0);

    // acquire lock while the event loop is not running
    {
        Loop::Lock lock(loop);
        // recursive locking doesn't deadlock
        Loop::Lock lock2(loop);
        Loop::Lock lock3(loop);
    }

    // acquire lock while the event loop is running
    std::thread thread(&Loop::runForever, &loop);
    {
        Loop::Lock lock(loop);
        uint64_t count = counter.count;
        usleep(1000);
        EXPECT_EQ(count, counter.count);
        // recursive locking doesn't deadlock
        Loop::Lock lock2(loop);
        Loop::Lock lock3(loop);
    }
    loop.exit();
    thread.join();
}

TEST(EventLoopTest, lock_mutex) {
    Loop loop;
    Counter counter(loop);
    Timer::Monitor counterMonitor(loop, counter);

    // Locks mutually exclude each other
    uint64_t count = 0;
    std::thread thread1(lockAndIncrement100,
                        std::ref(loop), std::ref(count));
    std::thread thread2(lockAndIncrement100,
                        std::ref(loop), std::ref(count));
    std::thread thread3(lockAndIncrement100,
                        std::ref(loop), std::ref(count));
    counter.schedule(1500 * 1000);
    loop.runForever();
    thread1.join();
    thread2.join();
    thread3.join();
    EXPECT_EQ(300U, count);
}

TEST(EventLoopTest, constructor) {
    Loop loop;
    // nothing to test
}

TEST(EventLoopTest, destructor) {
    Loop loop;
    // nothing to test
}

TEST(EventLoopTest, runForever) {
    Loop loop;
    Counter counter(loop);
    counter.exitAfter = 9;
    Timer::Monitor counterMonitor(loop, counter);
    counter.schedule(0);
    loop.runForever();
    EXPECT_EQ(10U, counter.count);
}

TEST(EventLoopTest, exit) {
    // The test for runForever already tested exit from within an event.

    Loop loop;
    Counter counter(loop);
    Timer::Monitor counterMonitor(loop, counter);
    counter.schedule(0);

    // exit before run
    loop.exit();
    loop.runForever();
    EXPECT_LT(counter.count, 2U);

    // exit from another thread
    std::thread thread(&Loop::runForever, &loop);
    loop.exit();
    thread.join();
}

} // namespace LogCabin::Event::<anonymous>
} // namespace LogCabin::Event
} // namespace LogCabin
