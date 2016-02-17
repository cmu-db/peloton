/* Copyright (c) 2012-2014 Stanford University
 * Copyright (c) 2014 Diego Ongaro
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

#include <functional>
#include <pthread.h>

#include "Core/CompatAtomic.h"
#include "Core/Mutex.h"
#include "Core/Time.h"

#ifndef LOGCABIN_CORE_CONDITIONVARIABLE_H
#define LOGCABIN_CORE_CONDITIONVARIABLE_H

namespace LogCabin {
namespace Core {

/**
 * Similar to std::condition_variable but with improvements for testing,
 * support for monotonic clocks, and less buggy.
 *
 * For testing, you can set a callback to be called when the condition variable
 * is waited on; instead of waiting, this callback will be called. This
 * callback can, for example, change some shared state so that the calling
 * thread's condition is satisfied. It also counts how many times the condition
 * variable has been notified.
 *
 * The interface to this class is a subset of std::condition_variable:
 * - wait_for isn't exposed since it doesn't make much sense to me in light
 *   of spurious interrupts.
 * - wait_until returns void instead of cv_status since it's almost always
 *   clearer to check whether timeout has elapsed explicitly. Also, gcc 4.4
 *   doesn't have cv_status and uses bool instead.
 *
 * This class also goes through some trouble so that it can be used with
 * std::unique_lock<Core::Mutex>, since normal condition variables may only be
 * used with std::unique_lock<std::mutex>.
 *
 * All waiting on this class is done using a monotonic clock internally, so it
 * will not be affected by time jumps from, e.g., NTP. This implies that, if
 * you're actually waiting for a specific system time to come around, you might
 * end up with surprising behavior. If you're wondering why, Linux/POSIX
 * condition variables use a single clock for all waiters, whereas C++11 uses
 * an impossible-to-implement interface where different waiters can use
 * different clocks.
 */
class ConditionVariable {
  public:
    ConditionVariable();
    ~ConditionVariable();
    void notify_one();
    void notify_all();
    void wait(std::unique_lock<std::mutex>& lockGuard);
    void wait(std::unique_lock<Core::Mutex>& lockGuard);

    // std::mutex and SteadyClock
    void
    wait_until(std::unique_lock<std::mutex>& lockGuard,
               const Core::Time::SteadyClock::time_point& abs_time);

    // std::mutex and any clock: calls std::mutex and SteadyClock variant
    template<typename Clock, typename Duration>
    void
    wait_until(std::unique_lock<std::mutex>& lockGuard,
               const std::chrono::time_point<Clock, Duration>& abs_time) {
        std::chrono::time_point<Clock, Duration> now = Clock::now();
        std::chrono::time_point<Clock, Duration> wake = abs_time;
        // Clamp to wake to [now - hour, now + hour] to avoid overflow.
        // See related http://gcc.gnu.org/bugzilla/show_bug.cgi?id=58931
        if (abs_time < now)
            wake = now - std::chrono::hours(1);
        else if (abs_time > now + std::chrono::hours(1))
            wake = now + std::chrono::hours(1);
        Core::Time::SteadyClock::time_point steadyNow =
            Core::Time::SteadyClock::now();
        Core::Time::SteadyClock::time_point steadyWake =
            steadyNow + (wake - now);
        wait_until(lockGuard, steadyWake);
    }

    // Core::Mutex and any clock: calls std::mutex and any clock variant
    template<typename Clock, typename Duration>
    void
    wait_until(std::unique_lock<Core::Mutex>& lockGuard,
               const std::chrono::time_point<Clock, Duration>& abs_time) {
        Core::Mutex& mutex(*lockGuard.mutex());
        if (mutex.callback)
            mutex.callback();
        assert(lockGuard);
        std::unique_lock<std::mutex> stdLockGuard(mutex.m,
                                                  std::adopt_lock_t());
        lockGuard.release();
        wait_until(stdLockGuard, abs_time);
        assert(stdLockGuard);
        lockGuard = std::unique_lock<Core::Mutex>(mutex, std::adopt_lock_t());
        stdLockGuard.release();
        if (mutex.callback)
            mutex.callback();
    }

  private:
    /// Underlying condition variable.
    pthread_cond_t cv;
    /**
     * This function will be called with the lock released during every
     * invocation of wait/wait_until. No wait will actually occur; this is only
     * used for unit testing.
     */
    std::function<void()> callback;
  public:
    /**
     * The number of times this condition variable has been notified.
     */
    std::atomic<uint64_t> notificationCount;
    /**
     * In the last call to wait_until, the timeout that the caller provided (in
     * terms of SteadyClock).
     * This is used in some unit tests to check that timeouts are set
     * correctly.
     */
    Core::Time::SteadyClock::time_point lastWaitUntil;
};

} // namespace LogCabin::Core
} // namespace LogCabin

#endif // LOGCABIN_CORE_CONDITIONVARIABLE_H
