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

#include <cmath>
#include <gtest/gtest.h>

#include "build/Protocol/ServerStats.pb.h"
#include "Core/ProtoBuf.h"
#include "Core/RollingStat.h"

namespace LogCabin {
namespace {

typedef Core::RollingStat::TimePoint TimePoint;

TEST(CoreRollingStatTest, basics) {
    Core::RollingStat stat;
    stat.push(0);
    stat.push(1);
    stat.push(2);
    stat.push(3);

    EXPECT_EQ(1.5, stat.getAverage());
    EXPECT_EQ(4U, stat.getCount());
    EXPECT_EQ(2.125, stat.getEWMA2());
    EXPECT_EQ(1.265625, stat.getEWMA4());
    EXPECT_EQ(0U, stat.getExceptionalCount());
    EXPECT_EQ(3U, stat.getLast());
    EXPECT_EQ(0U, stat.getMin());
    EXPECT_EQ(3U, stat.getMax());
    EXPECT_EQ(6U, stat.getSum());
    EXPECT_EQ(sqrt(1.25), stat.getStdDev());

    Protocol::RollingStat pb;
    stat.updateProtoBuf(pb);
    EXPECT_EQ("average: 1.5 "
              "count: 4 "
              "ewma2: 2.125 "
              "ewma4: 1.265625 "
              "exceptional_count: 0 "
              "last: 3 "
              "min: 0 "
              "max: 3 "
              "sum: 6 "
              "stddev: 1.1180339887498949",
              pb);
}

TEST(CoreRollingStatTest, initial) {
    Core::RollingStat stat;
    EXPECT_EQ(0, stat.getAverage());
    EXPECT_EQ(0U, stat.getCount());
    EXPECT_EQ(0, stat.getEWMA2());
    EXPECT_EQ(0, stat.getEWMA4());
    EXPECT_EQ(0U, stat.getExceptionalCount());
    EXPECT_EQ(0U, stat.getLast());
    EXPECT_EQ(0U, stat.getMin());
    EXPECT_EQ(0U, stat.getMax());
    EXPECT_EQ(0U, stat.getSum());
    EXPECT_EQ(0, stat.getStdDev());
}

TEST(CoreRollingStatTest, exceptional) {
    Core::RollingStat stat;
    stat.noteExceptional(TimePoint() + std::chrono::hours(1),
                         33);
    stat.noteExceptional(TimePoint() + std::chrono::hours(2),
                         39);
    stat.noteExceptional(TimePoint() + std::chrono::hours(3),
                         36);
    stat.noteExceptional(TimePoint() + std::chrono::hours(4),
                         32);
    stat.noteExceptional(TimePoint() + std::chrono::hours(5),
                         34);
    stat.noteExceptional(TimePoint() + std::chrono::hours(6),
                         17);
    EXPECT_EQ(6U, stat.getExceptionalCount());
    auto e = stat.getLastExceptional();
    ASSERT_EQ(5U, e.size());
    EXPECT_EQ(TimePoint() + std::chrono::hours(6), e.at(0).first);
    EXPECT_EQ(17U, e.at(0).second);
    EXPECT_EQ(TimePoint() + std::chrono::hours(2), e.at(4).first);
    EXPECT_EQ(39U, e.at(4).second);

    // Extra parenthesis needed to make these variable definitions as opposed
    // to function declarations (clang gives nice warning here, gcc doesn't).
    Core::Time::SystemClock::Mocker m1(
        (Core::Time::SystemClock::time_point()));
    Core::Time::SteadyClock::Mocker m2(
        (Core::Time::SteadyClock::time_point()));

    Protocol::RollingStat pb;
    stat.updateProtoBuf(pb);
    EXPECT_EQ("count: 0 "
              "exceptional_count: 6 "
              "last_exceptional { "
              "    when: 21600000000000 "
              "    value: 17 "
              "} "
              "last_exceptional { "
              "    when: 18000000000000 "
              "    value: 34 "
              "} "
              "last_exceptional { "
              "    when: 14400000000000 "
              "    value: 32 "
              "} "
              "last_exceptional { "
              "    when: 10800000000000 "
              "    value: 36 "
              "} "
              "last_exceptional { "
              "    when:  7200000000000 "
              "    value: 39 "
              "} ",
              pb);
}

// updateProtobuf tested with basics and exceptional

} // namespace LogCabin::<anonymous>
} // namespace LogCabin
