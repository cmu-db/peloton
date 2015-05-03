/**
 * @brief Test cases for sequential scan node.
 *
 * Copyright(c) 2015, CMU
 */

#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "common/types.h"

#include "harness.h"

namespace nstore {
namespace test {

// Sequential scan with predicate.
// The table being scanned has more than one tile group. i.e. the vertical
// paritioning changes midway.
TEST(SeqScanTests, TwoTileGroupsWithPredicateTest) {
  //TODO Implement.
}

} // namespace test
} // namespace nstore
