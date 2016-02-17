/* Copyright (c) 2011-2014 Stanford University
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

#include <cstring>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>

#include "Core/Debug.h"
#include "Event/Loop.h"
#include "Event/Timer.h"

namespace LogCabin {
namespace Event {

namespace {

/// Helper for constructor.
int
createTimerFd()
{
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK|TFD_CLOEXEC);
    if (fd < 0) {
        PANIC("Could not create timerfd: %s", strerror(errno));
    }
    return fd;
}

} // anonymous namespace

//// class Timer::Monitor ////

Timer::Monitor::Monitor(Event::Loop& eventLoop, Timer& timer)
    : File::Monitor(eventLoop, timer, EPOLLIN|EPOLLET)
{
}

Timer::Monitor::~Monitor()
{
}

//// class Timer ////

Timer::Timer()
    : Event::File(createTimerFd())
{
}

Timer::~Timer()
{
}

void
Timer::schedule(uint64_t nanoseconds)
{
    // avoid accidental de-schedules: epoll's semantics are that a timer for 0
    // seconds and 0 nanoseconds will never fire.
    if (nanoseconds == 0)
        nanoseconds = 1;

    const uint64_t nanosPerSecond = 1000 * 1000 * 1000;
    struct itimerspec newValue;
    memset(&newValue, 0, sizeof(newValue));
    newValue.it_value.tv_sec  = nanoseconds / nanosPerSecond;
    newValue.it_value.tv_nsec = nanoseconds % nanosPerSecond;
    int r = timerfd_settime(fd, 0, &newValue, NULL);
    if (r != 0) {
        PANIC("Could not set timer to +%luns: %s",
              nanoseconds,
              strerror(errno));
    }
}

void
Timer::scheduleAbsolute(Core::Time::SteadyClock::time_point timeout)
{
    static_assert(Core::Time::STEADY_CLOCK_ID == CLOCK_MONOTONIC,
                  "scheduleAbsolute assumes SteadyClock uses CLOCK_MONOTONIC");
    struct itimerspec newValue;
    memset(&newValue, 0, sizeof(newValue));
    newValue.it_value = Core::Time::makeTimeSpec(timeout);
    // Will get EINVAL on negative times, might as well use 0.000000001.
    if (newValue.it_value.tv_sec < 0) {
        newValue.it_value.tv_sec = 0;
        newValue.it_value.tv_nsec = 1;
    }
    // avoid accidental de-schedules: epoll's semantics are that a timer for 0
    // seconds and 0 nanoseconds will never fire.
    if (newValue.it_value.tv_sec == 0 && newValue.it_value.tv_nsec == 0)
        newValue.it_value.tv_nsec = 1;

    int r = timerfd_settime(fd, TFD_TIMER_ABSTIME, &newValue, NULL);
    if (r != 0) {
        PANIC("Could not set timer to %ld.%09ld: %s",
              newValue.it_value.tv_sec,
              newValue.it_value.tv_nsec,
              strerror(errno));
    }
}

void
Timer::deschedule()
{
    struct itimerspec newValue;
    memset(&newValue, 0, sizeof(newValue));
    int r = timerfd_settime(fd, 0, &newValue, NULL);
    if (r != 0)
        PANIC("Could not deschedule timer: %s", strerror(errno));
}

bool
Timer::isScheduled() const
{
    // Unfortunately, timerfd_gettime seems to return 0 when an absolute time
    // has already elapsed. See note on isScheduled() in header file.
    struct itimerspec currentValue;
    int r = timerfd_gettime(fd, &currentValue);
    if (r != 0)
        PANIC("Could not get timer: %s", strerror(errno));
    return (currentValue.it_value.tv_sec != 0 ||
            currentValue.it_value.tv_nsec != 0);
}

void
Timer::handleFileEvent(uint32_t events)
{
    handleTimerEvent();
}

} // namespace LogCabin::Event
} // namespace LogCabin
