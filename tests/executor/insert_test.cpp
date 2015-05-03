/**
 * @brief Test cases for insert node.
 *
 * Copyright(c) 2015, CMU
 */

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "common/value.h"
#include "common/value_factory.h"
#include "executor/concat_executor.h"
#include "executor/logical_tile.h"
#include "executor/logical_tile_factory.h"
#include "planner/insert_node.h"
#include "storage/backend_vm.h"
#include "storage/tile.h"
#include "storage/tile_group.h"

#include "executor/executor_tests_util.h"

namespace nstore {
namespace test {

// Insert a tuple into a table
TEST(InsertTests, BasicTest) {

}

} // namespace test
} // namespace nstore
