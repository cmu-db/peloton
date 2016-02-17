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

#include <gtest/gtest.h>

#include "Client/Backoff.h"

namespace LogCabin {
namespace Client {
namespace {

typedef Backoff::Clock Clock;
typedef Backoff::TimePoint TimePoint;

TEST(ClientBackoffTest, nocrash) {
    Backoff backoff(3, 1000 * 1000);
    backoff.delayAndBegin(TimePoint::max());
    backoff.delayAndBegin(TimePoint::max());
    backoff.delayAndBegin(TimePoint::max());
    backoff.delayAndBegin(TimePoint::max());
    backoff.delayAndBegin(TimePoint::min());
    backoff.delayAndBegin(TimePoint::max());
}

TEST(ClientBackoffTest, basics_TimingSensitive) {
    Backoff backoff(2, 12 * 1000 * 1000);
    TimePoint t1 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    //TimePoint t2 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    TimePoint t3 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // delay 12ms
    TimePoint t4 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    TimePoint t5 = Clock::now();
    std::chrono::milliseconds slop(5);
    EXPECT_GT(t1 + slop, t3);
    EXPECT_LT(t3 + std::chrono::milliseconds(12), t4);
    EXPECT_GT(t3 + std::chrono::milliseconds(12) + slop, t4);
    EXPECT_GT(t4 + slop, t5);
}

TEST(ClientBackoffTest, timeout_TimingSensitive) {
    Backoff backoff(2, 12 * 1000 * 1000);
    TimePoint t1 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    //TimePoint t2 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    //TimePoint t3 = Clock::now();
    backoff.delayAndBegin(TimePoint::min()); // immediate
    //TimePoint t4 = Clock::now();
    backoff.delayAndBegin(Clock::now()); // immediate
    TimePoint t5 = Clock::now();
    backoff.delayAndBegin(Clock::now() +
                          std::chrono::milliseconds(4)); // delay 4 ms
    TimePoint t6 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // delay 8ms
    TimePoint t7 = Clock::now();
    backoff.delayAndBegin(TimePoint::max()); // immediate
    TimePoint t8 = Clock::now();
    std::chrono::milliseconds slop(5);
    EXPECT_GT(t1 + slop, t5);
    EXPECT_LT(t5 + std::chrono::milliseconds(4), t6);
    EXPECT_GT(t5 + std::chrono::milliseconds(4) + slop, t6);
    EXPECT_LT(t5 + std::chrono::milliseconds(12), t7);
    EXPECT_GT(t5 + std::chrono::milliseconds(12) + slop, t7);
    EXPECT_GT(t7 + slop, t8);
}

} // namespace LogCabin::Client::<anonymous>
} // namespace LogCabin::Client
} // namespace LogCabin
