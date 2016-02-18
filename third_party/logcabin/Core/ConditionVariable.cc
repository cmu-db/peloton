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

#include <cstring>

#include "Core/ConditionVariable.h"
#include "Core/Debug.h"

namespace LogCabin {
namespace Core {

ConditionVariable::ConditionVariable()
    : cv()
    , callback()
    , notificationCount(0)
    , lastWaitUntil()
{
    // Note that all these pthread_cond* functions return errors in the return
    // code, NOT using errno.
    pthread_condattr_t cvattr;
    int r = pthread_condattr_init(&cvattr);
    if (r != 0)
        PANIC("pthread_condattr_init failed: %s", strerror(r));
    // glibc as of 2014 only supports CLOCK_MONOTONIC or CLOCK_REALTIME here,
    // others return EINVAL. See glibc/nptl/pthread_condattr_setclock.c.
    r = pthread_condattr_setclock(&cvattr, Core::Time::STEADY_CLOCK_ID);
    if (r != 0)
        PANIC("pthread_condattr_setclock failed: %s", strerror(r));
    r = pthread_cond_init(&cv, &cvattr);
    if (r != 0)
        PANIC("pthread_cond_init failed: %s", strerror(r));
    r = pthread_condattr_destroy(&cvattr);
    if (r != 0)
        PANIC("pthread_condattr_destroy failed: %s", strerror(r));
}

ConditionVariable::~ConditionVariable()
{
    int r = pthread_cond_destroy(&cv);
    if (r != 0)
        PANIC("pthread_cond_destroy failed: %s", strerror(r));
}

void
ConditionVariable::notify_one()
{
    ++notificationCount;
    int r = pthread_cond_signal(&cv);
    if (r != 0)
        PANIC("pthread_cond_signal failed: %s", strerror(r));
}

void
ConditionVariable::notify_all()
{
    ++notificationCount;
    int r = pthread_cond_broadcast(&cv);
    if (r != 0)
        PANIC("pthread_cond_broadcast failed: %s", strerror(r));
}

void
ConditionVariable::wait(std::unique_lock<std::mutex>& lockGuard)
{
    if (callback) {
        lockGuard.unlock();
        callback();
        lockGuard.lock();
    } else {
        pthread_mutex_t* mutex = lockGuard.mutex()->native_handle();
        int r = pthread_cond_wait(&cv, mutex);
        if (r != 0)
            PANIC("pthread_cond_wait failed: %s", strerror(r));
    }
}

void
ConditionVariable::wait(std::unique_lock<Core::Mutex>& lockGuard)
{
    Core::Mutex& mutex(*lockGuard.mutex());
    if (mutex.callback)
        mutex.callback();
    assert(lockGuard);
    std::unique_lock<std::mutex> stdLockGuard(mutex.m,
                                              std::adopt_lock_t());
    lockGuard.release();
    wait(stdLockGuard);
    assert(stdLockGuard);
    lockGuard = std::unique_lock<Core::Mutex>(mutex, std::adopt_lock_t());
    stdLockGuard.release();
    if (mutex.callback)
        mutex.callback();
}

void
ConditionVariable::wait_until(
            std::unique_lock<std::mutex>& lockGuard,
            const Core::Time::SteadyClock::time_point& abs_time)
{
    lastWaitUntil = abs_time;
    if (callback) {
        lockGuard.unlock();
        callback();
        lockGuard.lock();
    } else {
        Core::Time::SteadyClock::time_point now =
            Core::Time::SteadyClock::now();
        Core::Time::SteadyClock::time_point wake =
            std::min(abs_time, now + std::chrono::hours(1));
        if (wake < now)
            return;
        struct timespec wakespec = Core::Time::makeTimeSpec(wake);
        pthread_mutex_t* mutex = lockGuard.mutex()->native_handle();
        int r = pthread_cond_timedwait(&cv, mutex, &wakespec);
        switch (r) {
            case 0:
                break;
            case ETIMEDOUT:
                break;
            default:
                PANIC("pthread_cond_timedwait failed: %s", strerror(r));
        }
    }
}

} // namespace LogCabin::Core
} // namespace LogCabin
