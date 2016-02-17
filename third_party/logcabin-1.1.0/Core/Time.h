/* Copyright (c) 2011-2012 Stanford University
 * Copyright (c) 2014-2015 Diego Ongaro
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

#include <cassert>
#include <chrono>
#include <iostream>
#include <time.h>

#include "Core/StringUtil.h"

#ifndef LOGCABIN_CORE_TIME_H
#define LOGCABIN_CORE_TIME_H

namespace LogCabin {
namespace Core {
namespace Time {

/**
 * Convert a C++11 time point into a POSIX timespec.
 * \param when
 *      Time point to convert.
 * \return
 *      Time in seconds and nanoseconds relative to the Clock's epoch.
 */
template<typename Clock, typename Duration>
struct timespec
makeTimeSpec(const std::chrono::time_point<Clock, Duration>& when)
{
    std::chrono::nanoseconds::rep nanosSinceEpoch =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
           when.time_since_epoch()).count();
    struct timespec ts;
    ts.tv_sec  = nanosSinceEpoch / 1000000000L;
    ts.tv_nsec = nanosSinceEpoch % 1000000000L;
    // tv_nsec must always be in range [0, 1e9)
    if (nanosSinceEpoch < 0) {
        ts.tv_sec  -= 1;
        ts.tv_nsec += 1000000000L;
    }
    return ts;
}

/**
 * Wall clock in nanosecond granularity.
 * Wrapper around clock_gettime(CLOCK_REALTIME).
 * Usually, you'll want to access this through #SystemClock.
 *
 * This is preferred over std::chrono::system_clock for earlier libstdc++
 * versions, since those use only a microsecond granularity.
 */
struct CSystemClock {
    typedef std::chrono::nanoseconds duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<CSystemClock, duration> time_point;

    static const bool is_steady = false;
    static time_point now();
};

/**
 * The clock used by CSteadyClock. For now (2014), we can't use
 * CLOCK_MONOTONIC_RAW in condition variables since glibc doesn't support that,
 * so stick with CLOCK_MONOTONIC. This rate of this clock may change due to NTP
 * adjustments, but at least it won't jump.
 */
const clockid_t STEADY_CLOCK_ID = CLOCK_MONOTONIC;

/**
 * Monotonic clock in nanosecond granularity.
 * Wrapper around clock_gettime(STEADY_CLOCK_ID = CLOCK_MONOTONIC).
 * Usually, you'll want to access this through #SteadyClock.
 *
 * This is preferred over std::chrono::monotonic_clock and
 * std::chrono::steady_clock for earlier libstdc++ versions, since those use
 * are not actually monotonic (they're typedefed to system_clock).
 */
struct CSteadyClock {
    typedef std::chrono::nanoseconds duration;
    typedef duration::rep rep;
    typedef duration::period period;
    typedef std::chrono::time_point<CSteadyClock, duration> time_point;
    static const bool is_steady = true;

    static time_point now();
};


/**
 * Reads the current time. This time may not correspond to wall time, depending
 * on the underlying BaseClock. This class gives unit tests a way to fake the
 * current time.
 */
template<typename _BaseClock>
struct MockableClock
{
    typedef _BaseClock BaseClock;
    typedef typename BaseClock::duration duration;
    typedef typename BaseClock::rep rep;
    typedef typename BaseClock::period period;
    typedef typename BaseClock::time_point time_point;

    // libstdc++ 4.7 renamed monotonic_clock to steady_clock to conform with
    // C++11. This file doesn't use a BaseClock from libstdc++ before 4.8, so
    // it's ok to just assume is_steady is present.
    static const bool is_steady = BaseClock::is_steady;

    static time_point now() {
        if (useMockValue)
            return mockValue;
        else
            return BaseClock::now();
    }

    static bool useMockValue;
    static time_point mockValue;

    /// RAII class to mock out the clock and then restore it.
    struct Mocker {
        explicit Mocker(time_point value = now()) {
            assert(!useMockValue);
            useMockValue = true;
            mockValue = value;
        }
        ~Mocker() {
            useMockValue = false;
        }
    };
};

template<typename BaseClock>
bool
MockableClock<BaseClock>::useMockValue = false;

template<typename BaseClock>
typename MockableClock<BaseClock>::time_point
MockableClock<BaseClock>::mockValue;

/**
 * The best available clock on this system for uses where a steady, monotonic
 * clock is desired.
 */
// libstdc++ 4.7 renamed monotonic_clock to steady_clock to conform with C++11.
// libstdc++ 4.8 seems to be the first version where std::chrono::steady_clock
// is usable with nanosecond granularity.
// Clang with libstdc++ gives us no way to check the version,
// so we just use CSteadyClock in that case.
// Clang with libc++ is known not to work at _LIBCPP_VERSION 1101 and hasn't
// been tried with other versions (use CSteadyClock).
#if __clang__ || (__GNUC__ == 4 && __GNUC_MINOR__ < 8)
typedef MockableClock<CSteadyClock> SteadyClock;
#else
typedef MockableClock<std::chrono::steady_clock> SteadyClock;
#endif

/**
 * A clock that reads wall time and is affected by NTP adjustments.
 */
// libstdc++ 4.8 seems to be the first version where std::chrono::system_clock
// has nanosecond granularity.
// Clang with libstdc++ gives us no way to check the version,
// so we just use CSystemClock in that case.
// Clang with libc++ is known not to work at _LIBCPP_VERSION 1101 and hasn't
// been tried with other versions (use CSteadyClock).
#if __clang__ || (__GNUC__ == 4 && __GNUC_MINOR__ < 8)
typedef MockableClock<CSystemClock> SystemClock;
#else
typedef MockableClock<std::chrono::system_clock> SystemClock;
#endif

/**
 * Convert a human-readable description of a time duration into a number of
 * nanoseconds.
 * \param description
 *      Something like 10, 10s, -200ms, 3us, or -999ns. With no units, defaults
 *      to seconds. May be negative.
 *      Allowed units:
 *          ns, nanosecond(s),
 *          ms, millisecond(s),
 *          s, second(s),
 *          min, minute(s),
 *          h, hr, hour(s),
 *          d, day(s),
 *          w, wk, week(s),
 *          mo, month(s),
 *          y, yr, year(s).
 * \return
 *      Number of nanoseconds, capped to the range of a signed 64-bit integer.
 * \throw std::runtime_error
 *      If description could not be parsed successfully.
 */
int64_t parseSignedDuration(const std::string& description);

/**
 * Convert a human-readable description of a time duration into a number of
 * nanoseconds.
 * \param description
 *      Something like 10, 10s, 200ms, 3us, or 999ns. With no units, defaults
 *      to seconds. May not be negative.
 *      Allowed units:
 *          ns, nanosecond(s),
 *          ms, millisecond(s),
 *          s, second(s),
 *          min, minute(s),
 *          h, hr, hour(s),
 *          d, day(s),
 *          w, wk, week(s),
 *          mo, month(s),
 *          y, yr, year(s).
 * \return
 *      Number of nanoseconds, capped on the high end to the range of a signed
 *      64-bit integer.
 * \throw std::runtime_error
 *      If description could not be parsed successfully.
 */
uint64_t parseNonNegativeDuration(const std::string& description);

/**
 * Read the CPU's cycle counter.
 * This is useful for benchmarking.
 */
static __inline __attribute__((always_inline))
uint64_t
rdtsc()
{
    uint32_t lo, hi;
    __asm__ __volatile__("rdtsc" : "=a" (lo), "=d" (hi));
    return ((uint64_t(hi) << 32) | lo);
}

/**
 * Block the calling thread until the given time.
 */
void sleep(SteadyClock::time_point wake);

/**
 * Block the calling thread for the given duration.
 */
void sleep(std::chrono::nanoseconds duration);

/**
 * Used to convert one or more SteadyClock::time_point values into values of
 * the SystemClock. Using the same instance for many conversions is more
 * efficient, since the current time only has to be queried once for each clock
 * in the constructor.
 */
class SteadyTimeConverter {
  public:
    /**
     * Constructor.
     */
    SteadyTimeConverter();

    /**
     * Return the given time according the system clock (assuming no time
     * jumps).
     */
    SystemClock::time_point
    convert(SteadyClock::time_point when);

    /**
     * Return the given time in nanoseconds since the Unix epoch according the
     * system clock (assuming no time jumps).
     */
    int64_t
    unixNanos(SteadyClock::time_point when);

  private:
    /**
     * Time this object was constructed according to the SteadyClock.
     */
    SteadyClock::time_point steadyNow;
    /**
     * Time this object was constructed according to the SystemClock.
     */
    SystemClock::time_point systemNow;
};

} // namespace LogCabin::Core::Time
} // namespace LogCabin::Core
} // namespace LogCabin

namespace std {

// The following set of functions prints duration values in a human-friendly
// way, including their units.

std::ostream&
operator<<(std::ostream& os,
           const std::chrono::nanoseconds& duration);
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::microseconds& duration);
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::milliseconds& duration);
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::seconds& duration);
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::minutes& duration);
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::hours& duration);

/**
 * Prints std::time_point values in a way that is useful for unit tests.
 */
template<typename Clock, typename Duration>
std::ostream&
operator<<(std::ostream& os,
           const std::chrono::time_point<Clock, Duration>& timePoint) {
    typedef std::chrono::time_point<Clock, Duration> TimePoint;
    using LogCabin::Core::StringUtil::format;

    if (timePoint == TimePoint::min())
        return os << "TimePoint::min()";
    if (timePoint == TimePoint::max())
        return os << "TimePoint::max()";

    struct timespec ts = LogCabin::Core::Time::makeTimeSpec(timePoint);
    return os << format("%ld.%09ld", ts.tv_sec, ts.tv_nsec);
}

} // namespace std

#endif /* LOGCABIN_CORE_TIME_H */
