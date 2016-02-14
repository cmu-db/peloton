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

#include "include/LogCabin/Client.h"
#include "include/LogCabin/Util.h"

namespace LogCabin {
namespace Client {
namespace {

TEST(ClientUtilTest, parseSignedDuration) {
    EXPECT_EQ(6U, Util::parseSignedDuration("6ns"));
    EXPECT_THROW(Util::parseSignedDuration("99 apples"),
                 Client::InvalidArgumentException);
    try {
        Util::parseSignedDuration("99 apples");
    } catch (const std::runtime_error& e) {
        EXPECT_STREQ("Invalid time description: could not parse units "
                     "from 99 apples",
                     e.what());
    }
}

TEST(ClientUtilTest, parseNonNegativeDuration) {
    EXPECT_EQ(6U, Util::parseNonNegativeDuration("6ns"));
    EXPECT_THROW(Util::parseNonNegativeDuration("99 apples"),
                 Client::InvalidArgumentException);
    EXPECT_THROW(Util::parseNonNegativeDuration("-6ns"),
                 Client::InvalidArgumentException);
}

} // namespace LogCabin::Client::<anonymous>
} // namespace LogCabin::Client
} // namespace LogCabin
