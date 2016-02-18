/* Copyright (c) 2014-2015 Diego Ongaro
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
#include <stdexcept>

#include "Core/StringUtil.h"
#include "Core/Time.h"

namespace LogCabin {
namespace Core {
namespace {

using StringUtil::toString;
using std::chrono::nanoseconds;

TEST(CoreTime, makeTimeSpec) {
    struct timespec s;
    s = Time::makeTimeSpec(Time::SystemClock::time_point::max());
    EXPECT_EQ(9223372036, s.tv_sec);
    EXPECT_EQ(854775807, s.tv_nsec);
    s = Time::makeTimeSpec(Time::SystemClock::time_point::min());
    EXPECT_EQ(-9223372037, s.tv_sec);
    EXPECT_EQ(145224192, s.tv_nsec);
    s = Time::makeTimeSpec(Time::SystemClock::now());
    EXPECT_LT(1417720382U, s.tv_sec); // 2014-12-04
    EXPECT_GT(1893456000U, s.tv_sec); // 2030-01-01
    s = Time::makeTimeSpec(Time::SystemClock::time_point() +
                           std::chrono::nanoseconds(50));
    EXPECT_EQ(0, s.tv_sec);
    EXPECT_EQ(50, s.tv_nsec);
    s = Time::makeTimeSpec(Time::SystemClock::time_point() -
                           std::chrono::nanoseconds(50));
    EXPECT_EQ(-1, s.tv_sec);
    EXPECT_EQ(999999950, s.tv_nsec);
}

TEST(CoreTime, SystemClock_nanosecondGranularity) {
    std::chrono::nanoseconds::rep nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
           Time::SystemClock::now().time_since_epoch()).count();
    if (nanos % 1000 == 0) { // second try
        nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
           Time::SystemClock::now().time_since_epoch()).count();
    }
    EXPECT_LT(0, nanos % 1000);
}

TEST(CoreTime, CSystemClock_now_increasing) {
    Time::CSystemClock::time_point a = Time::CSystemClock::now();
    Time::CSystemClock::time_point b = Time::CSystemClock::now();
    EXPECT_LT(a, b);
}

TEST(CoreTime, CSystemClock_now_progressTimingSensitive) {
    Time::CSystemClock::time_point a = Time::CSystemClock::now();
    usleep(1000);
    Time::CSystemClock::time_point b = Time::CSystemClock::now();
    EXPECT_LT(a, b);
    EXPECT_LT(a + std::chrono::microseconds(500), b);
    EXPECT_LT(b, a + std::chrono::microseconds(1500));
}


TEST(CoreTime, SystemClock_now_increasing) {
    Time::SystemClock::time_point a = Time::SystemClock::now();
    Time::SystemClock::time_point b = Time::SystemClock::now();
    EXPECT_LT(a, b);
}

TEST(CoreTime, SystemClock_now_progressTimingSensitive) {
    Time::SystemClock::time_point a = Time::SystemClock::now();
    usleep(1000);
    Time::SystemClock::time_point b = Time::SystemClock::now();
    EXPECT_LT(a, b);
    EXPECT_LT(a + std::chrono::microseconds(500), b);
    EXPECT_LT(b, a + std::chrono::microseconds(1500));
}

TEST(CoreTime, SteadyClock_nanosecondGranularity) {
    std::chrono::nanoseconds::rep nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(
           Time::SteadyClock::now().time_since_epoch()).count();
    if (nanos % 1000 == 0) { // second try
        nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
           Time::SteadyClock::now().time_since_epoch()).count();
    }
    EXPECT_LT(0, nanos % 1000);
}

TEST(CoreTime, CSteadyClock_now_increasing) {
    Time::CSteadyClock::time_point a = Time::CSteadyClock::now();
    Time::CSteadyClock::time_point b = Time::CSteadyClock::now();
    EXPECT_LT(a, b);
}

TEST(CoreTime, CSteadyClock_now_progressTimingSensitive) {
    Time::CSteadyClock::time_point a = Time::CSteadyClock::now();
    usleep(1000);
    Time::CSteadyClock::time_point b = Time::CSteadyClock::now();
    EXPECT_LT(a, b);
    EXPECT_LT(a + std::chrono::microseconds(500), b);
    EXPECT_LT(b, a + std::chrono::microseconds(1500));
}


TEST(CoreTime, SteadyClock_now_increasing) {
    Time::SteadyClock::time_point a = Time::SteadyClock::now();
    Time::SteadyClock::time_point b = Time::SteadyClock::now();
    EXPECT_LT(a, b);
}

TEST(CoreTime, SteadyClock_now_progressTimingSensitive) {
    Time::SteadyClock::time_point a = Time::SteadyClock::now();
    usleep(1000);
    Time::SteadyClock::time_point b = Time::SteadyClock::now();
    EXPECT_LT(a, b);
    EXPECT_LT(a + std::chrono::microseconds(500), b);
    EXPECT_LT(b, a + std::chrono::microseconds(1500));
}

TEST(CoreTime, parseSignedDuration) {
    EXPECT_EQ(10000000000L, Time::parseSignedDuration("10s"));
    EXPECT_EQ(182000000L, Time::parseSignedDuration("182ms"));
    EXPECT_EQ(9000L, Time::parseSignedDuration("9us"));
    EXPECT_EQ(9000L, Time::parseSignedDuration("9 us "));
    EXPECT_EQ(10L, Time::parseSignedDuration("10ns"));
    EXPECT_EQ(10L, Time::parseSignedDuration("10ns"));
    EXPECT_EQ(0L, Time::parseSignedDuration("0s"));
    EXPECT_EQ(0L, Time::parseSignedDuration("0"));
    EXPECT_THROW(Time::parseSignedDuration("10e"), std::runtime_error);
    EXPECT_THROW(Time::parseSignedDuration("10 seconds now"),
                 std::runtime_error);
    EXPECT_THROW(Time::parseSignedDuration(""),
                 std::runtime_error);
    EXPECT_THROW(Time::parseSignedDuration(" "),
                 std::runtime_error);
}

TEST(CoreTime, parseSignedDurationOverflowPositive) {
    int64_t nearly = 1L << 62L;
    EXPECT_LT(nearly,
              Time::parseSignedDuration("9223372036854775807 nanoseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("9223372036854775808 nanoseconds"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("9223372036854775 microseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("9223372036854776 microseconds"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("9223372036854 milliseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("9223372036855 milliseconds"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("9223372036 seconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("9223372037 seconds"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("153722867 minutes"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("153722868 minutes"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("2562047 hours"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("2562048 hours"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("106751 days"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("106752 days"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("15250 weeks"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("15251 weeks"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("3507 months"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("3508 months"));
    EXPECT_LT(nearly,
              Time::parseSignedDuration("292 years"));
    EXPECT_EQ(std::numeric_limits<int64_t>::max(),
              Time::parseSignedDuration("293 years"));
}

TEST(CoreTime, parseSignedDurationOverflowNegative) {
    int64_t nearly = (1L << 62L) * -1L;
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-9223372036854775808 nanoseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-9223372036854775809 nanoseconds"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-9223372036854775 microseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-9223372036854776 microseconds"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-9223372036854 milliseconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-9223372036855 milliseconds"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-9223372036 seconds"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-9223372037 seconds"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-153722867 minutes"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-153722868 minutes"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-2562047 hours"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-2562048 hours"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-106751 days"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-106752 days"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-15250 weeks"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-15251 weeks"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-3507 months"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-3508 months"));
    EXPECT_GT(nearly,
              Time::parseSignedDuration("-292 years"));
    EXPECT_EQ(std::numeric_limits<int64_t>::min(),
              Time::parseSignedDuration("-293 years"));
}

TEST(CoreTime, parseNonNegativeDuration) {
    EXPECT_EQ(31557600000000000UL,
              Time::parseNonNegativeDuration("1 year"));
    EXPECT_EQ(0UL,
              Time::parseNonNegativeDuration("0"));
    EXPECT_THROW(Time::parseNonNegativeDuration("-1 year"),
                 std::runtime_error);
}

TEST(CoreTime, rdtsc_increasing) {
    uint64_t a = Time::rdtsc();
    uint64_t b = Time::rdtsc();
    EXPECT_LT(a, b);
}

TEST(CoreTime, rdtsc_progressTimingSensitive) {
    uint64_t a = Time::rdtsc();
    usleep(1000);
    uint64_t b = Time::rdtsc();
    EXPECT_LT(a, b);
    EXPECT_LT(a + 1000 * 1000, b);
    EXPECT_LT(b, a + 10 * 1000 * 1000);
}

TEST(CoreTime, sleepAbsolute_immediate_TimingSensitive) {
    Time::SteadyClock::time_point start = Time::SteadyClock::now();
    Time::sleep(Time::SteadyClock::time_point::min());
    Time::sleep(Time::SteadyClock::time_point());
    Time::sleep(Time::SteadyClock::now() - std::chrono::milliseconds(1));
    Time::sleep(Time::SteadyClock::now());
    EXPECT_GT(start + std::chrono::milliseconds(5),
              Time::SteadyClock::now());
}

TEST(CoreTime, sleepAbsolute_later_TimingSensitive) {
    Time::SteadyClock::time_point start = Time::SteadyClock::now();
    Time::sleep(start + std::chrono::milliseconds(12));
    Time::SteadyClock::time_point end = Time::SteadyClock::now();
    EXPECT_LT(start + std::chrono::milliseconds(12), end);
    EXPECT_GT(start + std::chrono::milliseconds(17), end);
}

TEST(CoreTime, sleepRelative_immediate_TimingSensitive) {
    Time::SteadyClock::time_point start = Time::SteadyClock::now();
    Time::sleep(std::chrono::nanoseconds::min());
    Time::sleep(std::chrono::nanoseconds(-10));
    Time::sleep(std::chrono::nanoseconds(0));
    Time::sleep(std::chrono::nanoseconds(10));
    EXPECT_GT(start + std::chrono::milliseconds(5),
              Time::SteadyClock::now());
}

TEST(CoreTime, sleepRelative_later_TimingSensitive) {
    Time::SteadyClock::time_point start = Time::SteadyClock::now();
    Time::sleep(std::chrono::milliseconds(12));
    Time::SteadyClock::time_point end = Time::SteadyClock::now();
    EXPECT_LT(start + std::chrono::milliseconds(12), end);
    EXPECT_GT(start + std::chrono::milliseconds(17), end);
}

TEST(CoreTimeSteadyTimeConverter, convert) {
    Time::SteadyTimeConverter conv;
    EXPECT_EQ(conv.systemNow,
              conv.convert(conv.steadyNow));
    EXPECT_EQ(conv.systemNow + std::chrono::hours(1),
              conv.convert(conv.steadyNow + std::chrono::hours(1)));
    EXPECT_EQ(conv.systemNow - std::chrono::hours(1),
              conv.convert(conv.steadyNow - std::chrono::hours(1)));
    EXPECT_GT(Time::SystemClock::time_point(),
              conv.convert(Time::SteadyClock::time_point::min()));
    EXPECT_EQ(Time::SystemClock::time_point::max(),
              conv.convert(Time::SteadyClock::time_point::max()));
    EXPECT_EQ(Time::SystemClock::time_point::max(),
              conv.convert(Time::SteadyClock::time_point::max() -
                           std::chrono::hours(1)));
    EXPECT_LT(Time::SystemClock::time_point::min(),
              conv.convert(Time::SteadyClock::time_point::min() +
                           std::chrono::hours(1)));
    EXPECT_GT(Time::SystemClock::time_point(),
              conv.convert(Time::SteadyClock::time_point::min() +
                           std::chrono::hours(1)));
}

TEST(CoreTimeSteadyTimeConverter, unixNanos) {
    Time::SteadyTimeConverter conv;
    int64_t now = std::chrono::nanoseconds(
        conv.systemNow.time_since_epoch()).count();
    int64_t hour = 60L * 60 * 1000 * 1000 * 1000;
    EXPECT_EQ(now,
              conv.unixNanos(conv.steadyNow));
    EXPECT_EQ(now + hour,
              conv.unixNanos(conv.steadyNow + std::chrono::hours(1)));
    EXPECT_EQ(now - hour,
              conv.unixNanos(conv.steadyNow - std::chrono::hours(1)));
    EXPECT_GT(0,
              conv.unixNanos(Time::SteadyClock::time_point::min()));
    EXPECT_EQ(INT64_MAX,
              conv.unixNanos(Time::SteadyClock::time_point::max()));
    EXPECT_EQ(INT64_MAX,
              conv.unixNanos(Time::SteadyClock::time_point::max() -
                             std::chrono::hours(1)));
    EXPECT_LT(INT64_MIN,
              conv.unixNanos(Time::SteadyClock::time_point::min() +
                             std::chrono::hours(1)));
    EXPECT_GT(0,
              conv.unixNanos(Time::SteadyClock::time_point::min() +
                             std::chrono::hours(1)));
}

TEST(CoreTime, padFraction) {
    EXPECT_EQ("5 s", toString(nanoseconds(5000000000)));
    EXPECT_EQ("-5 s", toString(nanoseconds(-5000000000)));

    EXPECT_EQ("5.100 s", toString(nanoseconds(5100000000)));
    EXPECT_EQ("5.123456789 s", toString(nanoseconds(5123456789)));
    EXPECT_EQ("-5.100 s", toString(nanoseconds(-5100000000)));

    EXPECT_EQ("5.010 s", toString(nanoseconds(5010000000)));
    EXPECT_EQ("5.120 s", toString(nanoseconds(5120000000)));
    EXPECT_EQ("5.012345678 s", toString(nanoseconds(5012345678)));
    EXPECT_EQ("-5.010 s", toString(nanoseconds(-5010000000)));

    EXPECT_EQ("5.001 s", toString(nanoseconds(5001000000)));
    EXPECT_EQ("5.123 s", toString(nanoseconds(5123000000)));
    EXPECT_EQ("5.001234567 s", toString(nanoseconds(5001234567)));
    EXPECT_EQ("-5.001 s", toString(nanoseconds(-5001000000)));

    EXPECT_EQ("5.000100 s", toString(nanoseconds(5000100000)));
    EXPECT_EQ("5.123400 s", toString(nanoseconds(5123400000)));
    EXPECT_EQ("5.000123456 s", toString(nanoseconds(5000123456)));
    EXPECT_EQ("-5.000100 s", toString(nanoseconds(-5000100000)));

    EXPECT_EQ("5.000010 s", toString(nanoseconds(5000010000)));
    EXPECT_EQ("5.123450 s", toString(nanoseconds(5123450000)));
    EXPECT_EQ("5.000012345 s", toString(nanoseconds(5000012345)));
    EXPECT_EQ("-5.000010 s", toString(nanoseconds(-5000010000)));

    EXPECT_EQ("5.000001 s", toString(nanoseconds(5000001000)));
    EXPECT_EQ("5.123456 s", toString(nanoseconds(5123456000)));
    EXPECT_EQ("5.000001234 s", toString(nanoseconds(5000001234)));
    EXPECT_EQ("-5.000001 s", toString(nanoseconds(-5000001000)));

    EXPECT_EQ("5.000000100 s", toString(nanoseconds(5000000100)));
    EXPECT_EQ("5.123456700 s", toString(nanoseconds(5123456700)));
    EXPECT_EQ("5.000000123 s", toString(nanoseconds(5000000123)));
    EXPECT_EQ("-5.000000100 s", toString(nanoseconds(-5000000100)));

    EXPECT_EQ("5.000000010 s", toString(nanoseconds(5000000010)));
    EXPECT_EQ("5.123456780 s", toString(nanoseconds(5123456780)));
    EXPECT_EQ("5.000000012 s", toString(nanoseconds(5000000012)));
    EXPECT_EQ("-5.000000010 s", toString(nanoseconds(-5000000010)));

    EXPECT_EQ("5.000000001 s", toString(nanoseconds(5000000001)));
    EXPECT_EQ("5.123456789 s", toString(nanoseconds(5123456789)));
    EXPECT_EQ("5.000000001 s", toString(nanoseconds(5000000001)));
    EXPECT_EQ("-5.000000001 s", toString(nanoseconds(-5000000001)));
}

TEST(CoreTime, output_nanoseconds) {
    EXPECT_EQ("0 ns", toString(nanoseconds(0)));
    EXPECT_EQ("5 ns", toString(nanoseconds(5)));
}

TEST(CoreTime, output_microseconds) {
    EXPECT_EQ("5 us", toString(nanoseconds(5000)));
    EXPECT_EQ("5.001 us", toString(nanoseconds(5001)));
    EXPECT_EQ("5 us", toString(std::chrono::microseconds(5)));
}

TEST(CoreTime, output_milliseconds) {
    EXPECT_EQ("5 ms", toString(nanoseconds(5000000)));
    EXPECT_EQ("5.000001 ms", toString(nanoseconds(5000001)));
    EXPECT_EQ("5 ms", toString(std::chrono::microseconds(5000)));
    EXPECT_EQ("5.001 ms", toString(std::chrono::microseconds(5001)));
    EXPECT_EQ("5 ms", toString(std::chrono::milliseconds(5)));
}

TEST(CoreTime, output_seconds) {
    EXPECT_EQ("5 s", toString(nanoseconds(5000000000)));
    EXPECT_EQ("5.000000001 s", toString(nanoseconds(5000000001)));
    EXPECT_EQ("5 s", toString(std::chrono::microseconds(5000000)));
    EXPECT_EQ("5.000001 s", toString(std::chrono::microseconds(5000001)));
    EXPECT_EQ("5 s", toString(std::chrono::milliseconds(5000)));
    EXPECT_EQ("5.001 s", toString(std::chrono::milliseconds(5001)));
    EXPECT_EQ("5 s", toString(std::chrono::seconds(5)));
}

TEST(CoreTime, output_minutes) {
    EXPECT_EQ("300 s", toString(std::chrono::minutes(5)));
}

TEST(CoreTime, output_hours) {
    EXPECT_EQ("18000 s", toString(std::chrono::hours(5)));
}

TEST(CoreTime, output_negative) {
    EXPECT_EQ("-3 ns", toString(nanoseconds(-3)));
    EXPECT_EQ("-3.001 us", toString(nanoseconds(-3001)));
}

TEST(CoreTime, output_timepoint) {
    EXPECT_EQ("TimePoint::min()",
              toString(Time::SteadyClock::time_point::min()));
    EXPECT_EQ("TimePoint::max()",
              toString(Time::SystemClock::time_point::max()));
    EXPECT_LT(0.0, std::stold(toString(Time::SystemClock::now())));
}


} // namespace LogCabin::Core::<anonymous>
} // namespace LogCabin::Core
} // namespace LogCabin
