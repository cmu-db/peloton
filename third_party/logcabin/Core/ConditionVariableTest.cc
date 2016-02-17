/* Copyright (c) 2014 Stanford University
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

#include "Core/ConditionVariable.h"
#include "Core/Time.h"

namespace LogCabin {
namespace Core {
namespace {

typedef std::chrono::milliseconds ms;

// Many of these tests are very sensitive to timing and system load.
// That's just the nature of testing condition variables.

class CoreConditionVariableTest : public ::testing::Test {
    CoreConditionVariableTest()
        : mutex()
        , stdmutex()
        , cv()
        , ready(0)
        , done(0)
        , counter1(0)
        , counter2(0)
        , mutexCounter(0)
        , cond(false)
        , thread1()
        , thread2()
    {
        mutex.callback =
            std::bind(&CoreConditionVariableTest::incrementMutexCounter, this);
    }
    ~CoreConditionVariableTest()
    {
        cv.notify_all();
        if (thread1.joinable())
            thread1.join();
        if (thread2.joinable())
            thread2.join();
    }

    void wait()
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        ++ready;
        while (!cond) {
            cv.wait(lockGuard);
            ++counter2;
        }
        ++done;
    }

    void waitStdMutex()
    {
        std::unique_lock<std::mutex> lockGuard(stdmutex);
        ++ready;
        while (!cond) {
            cv.wait(lockGuard);
            ++counter2;
        }
        ++done;
    }

    void incrementCounter1()
    {
        ++counter1;
    }

    void incrementCounter2()
    {
        ++counter2;
    }

    void incrementMutexCounter()
    {
        ++mutexCounter;
    }

    void spinForReady(uint64_t until)
    {
        for (uint64_t i = 0; i < 100; ++i) {
            if (ready >= until)
                return;
            usleep(1000);
        }
        PANIC("timeout");
    }

    void sleepAndNotify(uint64_t ms, std::atomic<bool>& exit)
    {
        for (uint64_t i = 0; i < ms; ++i) {
            if (exit)
                return;
            usleep(1000);
        }
        cv.notify_all();
    }

    template<typename LockGuard, typename Clock, typename Duration>
    Duration
    timedWaitUntil(LockGuard& lockGuard,
                   const std::chrono::time_point<Clock, Duration>& abs_time,
                   uint64_t watchdogMs = 50)
    {
        std::atomic<bool> watchdogExit(false);
        std::thread watchdog(&CoreConditionVariableTest::sleepAndNotify,
                             this, watchdogMs, std::ref(watchdogExit));
        typename Clock::time_point start = Clock::now();
        cv.wait_until(lockGuard, abs_time);
        typename Clock::time_point end = Clock::now();
        watchdogExit = true;
        watchdog.join();
        return end - start;
    }

    Mutex mutex;
    std::mutex stdmutex;
    ConditionVariable cv;
    std::atomic<uint64_t> ready;
    std::atomic<uint64_t> done;
    std::atomic<uint64_t> counter1;
    std::atomic<uint64_t> counter2;
    std::atomic<uint64_t> mutexCounter;
    std::atomic<bool> cond;
    std::thread thread1;
    std::thread thread2;
};


TEST_F(CoreConditionVariableTest, notify_one_TimingSensitive) {
    cv.notify_one();
    EXPECT_EQ(1U, cv.notificationCount);
    thread1 = std::thread(&CoreConditionVariableTest::wait, this);
    thread2 = std::thread(&CoreConditionVariableTest::wait, this);
    spinForReady(2);
    EXPECT_EQ(0U, done);
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        cond = true;
        cv.notify_one();
    }
    EXPECT_EQ(2U, cv.notificationCount);
    usleep(1000);
    EXPECT_EQ(1U, counter2);
    EXPECT_EQ(1U, done);
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        cond = true;
        cv.notify_one();
    }
    usleep(1000);
    EXPECT_EQ(2U, counter2);
    EXPECT_EQ(2U, done);
    thread1.join();
    thread2.join();
}

TEST_F(CoreConditionVariableTest, notify_all_TimingSensitive) {
    cv.notify_all();
    EXPECT_EQ(1U, cv.notificationCount);
    thread1 = std::thread(&CoreConditionVariableTest::wait, this);
    thread2 = std::thread(&CoreConditionVariableTest::wait, this);
    spinForReady(2);
    EXPECT_EQ(0U, done);
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        cond = true;
        cv.notify_all();
    }
    EXPECT_EQ(2U, cv.notificationCount);
    thread1.join();
    thread2.join();
    EXPECT_EQ(2U, counter2);
    EXPECT_EQ(2U, done);
}

TEST_F(CoreConditionVariableTest, wait_stdmutex_callback) {
    cv.callback = std::bind(&CoreConditionVariableTest::incrementCounter1,
                            this);
    std::unique_lock<std::mutex> lockGuard(stdmutex);
    cv.wait(lockGuard);
    EXPECT_EQ(1U, counter1);
}

TEST_F(CoreConditionVariableTest, wait_stdmutex_real_TimingSensitive) {
    thread1 = std::thread(&CoreConditionVariableTest::waitStdMutex, this);
    spinForReady(1);
    EXPECT_EQ(0U, done);
    {
        std::unique_lock<std::mutex> lockGuard(stdmutex);
        cond = true;
        cv.notify_all();
    }
    thread1.join();
    EXPECT_EQ(1U, counter2);
}

TEST_F(CoreConditionVariableTest, wait_CoreMutex_callback) {
    cv.callback = std::bind(&CoreConditionVariableTest::incrementCounter1,
                            this);
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        cv.wait(lockGuard);
    }
    EXPECT_EQ(1U, counter1);
    EXPECT_EQ(4U, mutexCounter); // 2 from lock guard, 2 from wait
}

TEST_F(CoreConditionVariableTest, wait_CoreMutex_real_TimingSensitive) {
    thread1 = std::thread(&CoreConditionVariableTest::wait, this);
    spinForReady(1);
    EXPECT_EQ(0U, done);
    {
        std::unique_lock<Mutex> lockGuard(mutex);
        cond = true;
        cv.notify_all();
    }
    thread1.join();
    EXPECT_EQ(1U, counter2);
    EXPECT_EQ(6U, mutexCounter); // 4 from lock guards, 2 from wait
}


TEST_F(CoreConditionVariableTest, wait_until_stdmutex_callback) {
    cv.callback = std::bind(&CoreConditionVariableTest::incrementCounter1,
                            this);
    std::unique_lock<std::mutex> lockGuard(stdmutex);

    cv.wait_until(lockGuard, Time::SteadyClock::time_point::min());
    cv.wait_until(lockGuard, Time::SteadyClock::now() - ms(1));
    cv.wait_until(lockGuard, Time::SteadyClock::now());
    cv.wait_until(lockGuard, Time::SteadyClock::now() + ms(1));
    cv.wait_until(lockGuard, Time::SteadyClock::time_point::max());
    EXPECT_EQ(5U, counter1);
    counter1 = 0;

    cv.wait_until(lockGuard, Time::SystemClock::time_point::min());
    cv.wait_until(lockGuard, Time::SystemClock::now() - ms(1));
    cv.wait_until(lockGuard, Time::SystemClock::now());
    cv.wait_until(lockGuard, Time::SystemClock::now() + ms(1));
    cv.wait_until(lockGuard, Time::SystemClock::time_point::max());
    EXPECT_EQ(5U, counter1);
}

TEST_F(CoreConditionVariableTest, wait_until_stdmutex_real_TimingSensitive) {
    std::unique_lock<std::mutex> lockGuard(stdmutex);
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::time_point::min()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now() - ms(1)),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now() + ms(1)),
              ms(2));
    EXPECT_GT(timedWaitUntil(lockGuard, Time::SteadyClock::now() + ms(10), 2),
              ms(1));
    EXPECT_GT(timedWaitUntil(lockGuard,
                             Time::SteadyClock::time_point::max(), 2), ms(1));

    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::time_point::min()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now() - ms(1)),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now() + ms(1)),
              ms(2));
    EXPECT_GT(timedWaitUntil(lockGuard, Time::SystemClock::now() + ms(10), 2),
              ms(1));
    EXPECT_GT(timedWaitUntil(lockGuard,
                             Time::SystemClock::time_point::max(), 2), ms(1));
}

TEST_F(CoreConditionVariableTest, wait_until_CoreMutex_callback) {
    cv.callback = std::bind(&CoreConditionVariableTest::incrementCounter1,
                            this);
    {
        std::unique_lock<Core::Mutex> lockGuard(mutex);

        cv.wait_until(lockGuard, Time::SteadyClock::time_point::min());
        cv.wait_until(lockGuard, Time::SteadyClock::now() - ms(1));
        cv.wait_until(lockGuard, Time::SteadyClock::now());
        cv.wait_until(lockGuard, Time::SteadyClock::now() + ms(1));
        cv.wait_until(lockGuard, Time::SteadyClock::time_point::max());
        EXPECT_EQ(5U, counter1);
        counter1 = 0;

        cv.wait_until(lockGuard, Time::SystemClock::time_point::min());
        cv.wait_until(lockGuard, Time::SystemClock::now() - ms(1));
        cv.wait_until(lockGuard, Time::SystemClock::now());
        cv.wait_until(lockGuard, Time::SystemClock::now() + ms(1));
        cv.wait_until(lockGuard, Time::SystemClock::time_point::max());
        EXPECT_EQ(5U, counter1);
    }
    EXPECT_EQ(22U, mutexCounter); // 2 from lock guard, 20 from wait_until
}

TEST_F(CoreConditionVariableTest, wait_until_CoreMutex_real_TimingSensitive) {
    std::unique_lock<Core::Mutex> lockGuard(mutex);
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::time_point::min()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now() - ms(1)),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SteadyClock::now() + ms(1)),
              ms(2));
    EXPECT_GT(timedWaitUntil(lockGuard, Time::SteadyClock::now() + ms(10), 2),
              ms(1));
    EXPECT_GT(timedWaitUntil(lockGuard,
                             Time::SteadyClock::time_point::max(), 2), ms(1));

    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::time_point::min()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now() - ms(1)),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now()),
              ms(1));
    EXPECT_LT(timedWaitUntil(lockGuard, Time::SystemClock::now() + ms(1)),
              ms(2));
    EXPECT_GT(timedWaitUntil(lockGuard, Time::SystemClock::now() + ms(10), 2),
              ms(1));
    EXPECT_GT(timedWaitUntil(lockGuard,
                             Time::SystemClock::time_point::max(), 2), ms(1));
    lockGuard.unlock();
    EXPECT_EQ(26U, mutexCounter); // 2 from lock guard, 24 from wait_until
}

} // namespace LogCabin::Core::<anonymous>
} // namespace LogCabin::Core
} // namespace LogCabin
