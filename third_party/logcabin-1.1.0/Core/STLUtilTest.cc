/* Copyright (c) 2011-2012 Stanford University
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

#include <map>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "Core/STLUtil.h"

namespace LogCabin {
namespace Core {
namespace STLUtil {
namespace {

using std::map;
using std::pair;
using std::string;
using std::vector;

const map<int, string> empty {};

const map<int, string> digits {
    { 1, "one" },
    { 2, "two" },
    { 3, "three" },
};

TEST(CoreSTLUtilTest, sorted) {
    EXPECT_EQ((vector<int> {}),
              sorted(vector<int> {}));
    EXPECT_EQ((vector<int> { 1, 5, 7}),
              sorted(vector<int> {5, 1, 7}));
}

TEST(CoreSTLUtilTest, getKeys) {
    EXPECT_EQ((vector<int>{}),
              getKeys(empty));
    EXPECT_EQ((vector<int>{ 1, 2, 3 }),
              getKeys(digits));
}

TEST(CoreSTLUtilTest, getValues) {
    EXPECT_EQ((vector<string>{}),
              getValues(empty));
    EXPECT_EQ((vector<string>{ "one", "two", "three" }),
              getValues(digits));
}

TEST(CoreSTLUtilTest, getItems) {
    EXPECT_EQ((vector<pair<int, string>>{}),
              getItems(empty));
    EXPECT_EQ((vector<pair<int, string>>{
                    {1, "one"},
                    {2, "two"},
                    {3, "three"},
               }),
              getItems(digits));
}

} // namespace LogCabin::Core::STLUtil::<anonymous>
} // namespace LogCabin::Core::STLUtil
} // namespace LogCabin::Core
} // namespace LogCabin
