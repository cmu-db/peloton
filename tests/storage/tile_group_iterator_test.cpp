//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group_iterator_test.cpp
//
// Identification: tests/storage/tile_group_iterator_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "harness.h"

#include "backend/storage/data_table.h"
#include "backend/storage/tile_group.h"
#include "executor/executor_tests_util.h"
#include "backend/storage/tile_group_iterator.h"
#include "backend/concurrency/transaction_manager_factory.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TileGroupIterator Tests
//===--------------------------------------------------------------------===//

class TileGroupIteratorTests : public PelotonTest {};

TEST_F(TileGroupIteratorTests, BasicTest) {
  const int tuples_per_tilegroup = TESTS_TUPLES_PER_TILEGROUP;
  const int expected_tilegroup_count = 5;
  const int allocated_tilegroup_count = 6;
  const int tuple_count = tuples_per_tilegroup * expected_tilegroup_count;

  // Create a table and wrap it in logical tiles
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::unique_ptr<storage::DataTable> data_table(
      ExecutorTestsUtil::CreateTable(tuples_per_tilegroup, false));
  ExecutorTestsUtil::PopulateTable(txn, data_table.get(), tuple_count, false,
                                   false, true);
  txn_manager.CommitTransaction();

  storage::TileGroupIterator tile_group_itr(data_table.get());
  std::shared_ptr<storage::TileGroup> tile_group_ptr;
  int actual_tile_group_count = 0;
  while (tile_group_itr.Next(tile_group_ptr)) {
    if (tile_group_ptr.get() != nullptr) {
      actual_tile_group_count += 1;
    }
  }  // WHILE

  EXPECT_EQ(allocated_tilegroup_count, actual_tile_group_count);
}

}  // End test namespace
}  // End peloton namespace
