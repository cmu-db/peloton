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
#include <sys/time.h>

#include "Core/Util.h"
#include "Event/Loop.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Event {
namespace {

struct MyTimer : public Event::Timer {
    explicit MyTimer(Event::Loop& loop)
        : Timer()
        , eventLoop(loop)
        , triggerCount(0)
    {
    }
    void handleTimerEvent() {
        EXPECT_FALSE(isScheduled());
        ++triggerCount;
        eventLoop.exit();
    }
    Event::Loop& eventLoop;
    uint32_t triggerCount;
};

struct EventTimerTest : public ::testing::Test {
    EventTimerTest()
        : loop()
        , timer1(loop)
        , monitor(loop, timer1)
    {
    }
    Event::Loop loop;
    MyTimer timer1;
    Event::Timer::Monitor monitor;
};

TEST_F(EventTimerTest, constructor) {
}

TEST_F(EventTimerTest, destructor) {
    // Nothing to test.
}

TEST_F(EventTimerTest, schedule_immediate) {
    timer1.schedule(1000*1000);
    EXPECT_TRUE(timer1.isScheduled());
    timer1.schedule(0);
    loop.runForever();
    timer1.schedule(1000);
    loop.runForever();
    EXPECT_EQ(2U, timer1.triggerCount);
    EXPECT_FALSE(timer1.isScheduled());
}

typedef Core::Time::SystemClock Clock;
typedef Clock::time_point TimePoint;

uint64_t
diffMillis(TimePoint startTime, TimePoint endTime)
{
    return Core::Util::downCast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
                endTime - startTime).count());
}

TEST_F(EventTimerTest, schedule_timeElapsed_TimingSensitive) {
    TimePoint start = Clock::now();
    timer1.schedule(5 * 1000 * 1000); // 5ms
    EXPECT_TRUE(timer1.isScheduled());
    loop.runForever();
    EXPECT_EQ(1U, timer1.triggerCount);
    TimePoint end = Clock::now();
    uint64_t elapsedMillis = diffMillis(start, end);
    EXPECT_LE(5U, elapsedMillis);
    EXPECT_LE(elapsedMillis, 15U) <<
        "A 5ms timer took " << elapsedMillis << " ms to fire. "
        "Either something is misbehaving or your system is bogged down.";
    EXPECT_FALSE(timer1.isScheduled());
}

TEST_F(EventTimerTest, scheduleAbsolute_timeElapsed_TimingSensitive) {
    TimePoint start = Clock::now();
    timer1.scheduleAbsolute(Core::Time::SteadyClock::now() +
                            std::chrono::milliseconds(5));
    EXPECT_TRUE(timer1.isScheduled());
    loop.runForever();
    EXPECT_EQ(1U, timer1.triggerCount);
    TimePoint end = Clock::now();
    uint64_t elapsedMillis = diffMillis(start, end);
    EXPECT_LE(5U, elapsedMillis);
    EXPECT_LE(elapsedMillis, 15U) <<
        "A 5ms timer took " << elapsedMillis << " ms to fire. "
        "Either something is misbehaving or your system is bogged down.";
    EXPECT_FALSE(timer1.isScheduled());
}

TEST_F(EventTimerTest, scheduleAbsolute_min_TimingSensitive) {
    timer1.scheduleAbsolute(Core::Time::SteadyClock::time_point::min());
    // broken: EXPECT_TRUE(timer1.isScheduled());
    TimePoint start = Clock::now();
    loop.runForever();
    TimePoint end = Clock::now();
    uint64_t elapsedMillis = diffMillis(start, end);
    EXPECT_LE(elapsedMillis, 1U);
    EXPECT_EQ(1U, timer1.triggerCount);
    EXPECT_FALSE(timer1.isScheduled());
}

TEST_F(EventTimerTest, scheduleAbsolute_past_TimingSensitive) {
    timer1.scheduleAbsolute(Core::Time::SteadyClock::now() -
                            std::chrono::nanoseconds(3));
    // broken: EXPECT_TRUE(timer1.isScheduled());
    TimePoint start = Clock::now();
    loop.runForever();
    TimePoint end = Clock::now();
    uint64_t elapsedMillis = diffMillis(start, end);
    EXPECT_LE(elapsedMillis, 1U);
    EXPECT_EQ(1U, timer1.triggerCount);
    EXPECT_FALSE(timer1.isScheduled());
}

TEST_F(EventTimerTest, scheduleAbsolute_zero_TimingSensitive) {
    timer1.scheduleAbsolute(Core::Time::SteadyClock::time_point());
    // broken: EXPECT_TRUE(timer1.isScheduled());
    TimePoint start = Clock::now();
    loop.runForever();
    TimePoint end = Clock::now();
    uint64_t elapsedMillis = diffMillis(start, end);
    EXPECT_LE(elapsedMillis, 1U);
    EXPECT_EQ(1U, timer1.triggerCount);
    EXPECT_FALSE(timer1.isScheduled());
}

TEST_F(EventTimerTest, deschedule) {
    timer1.schedule(0);
    timer1.deschedule();
    // make sure it's ok to deschedule things that aren't scheduled
    timer1.deschedule();
    EXPECT_FALSE(timer1.isScheduled());
    MyTimer timer2(loop);
    Event::Timer::Monitor monitor2(loop, timer2);
    timer2.schedule(10);
    loop.runForever();
    EXPECT_EQ(0U, timer1.triggerCount);
    EXPECT_EQ(1U, timer2.triggerCount);
    EXPECT_FALSE(timer2.isScheduled());
}

TEST_F(EventTimerTest, isScheduled) {
    // Tested sufficiently in schedule, deschedule tests.
}

} // namespace LogCabin::Event::<anonymous>
} // namespace LogCabin::Event
} // namespace LogCabin
