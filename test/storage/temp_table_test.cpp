//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_test.cpp
//
// Identification: test/storage/data_table_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <vector>

#include "storage/temp_table.h"
#include "storage/tile.h"
#include "storage/tile_group.h"
#include "storage/tuple_iterator.h"
#include "type/value_factory.h"

#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// TempTable Tests
//===--------------------------------------------------------------------===//

class TempTableTests : public PelotonTest {};

TEST_F(TempTableTests, InsertTest) {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  auto pool = TestingHarness::GetInstance().GetTestingPool();

  catalog::Schema *schema = new catalog::Schema(
      {TestingExecutorUtil::GetColumnInfo(0), TestingExecutorUtil::GetColumnInfo(1),
       TestingExecutorUtil::GetColumnInfo(2),
       TestingExecutorUtil::GetColumnInfo(3)});

  // Create our TempTable
  storage::TempTable table(INVALID_OID, schema, true);
  EXPECT_EQ(0, table.GetTupleCount());

  std::vector<type::Value> values;

  // Then shove some tuples in it
  for (int i = 0; i < tuple_count; i++) {
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table.GetSchema(), true));
    auto val1 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 0));
    auto val2 = type::ValueFactory::GetIntegerValue(
        TestingExecutorUtil::PopulatedValue(i, 1));
    auto val3 = type::ValueFactory::GetDecimalValue(
        TestingExecutorUtil::PopulatedValue(i, 2));
    auto val4 = type::ValueFactory::GetVarcharValue("12345");
    tuple->SetValue(0, val1, pool);
    tuple->SetValue(1, val2, pool);
    tuple->SetValue(2, val3, pool);
    tuple->SetValue(3, val4, pool);
    table.InsertTuple(tuple.get());

    // Copy the first value so that we can just check that we are able to
    // correctly get the values back.
    values.push_back(val1);
  }

  // Make sure that we have the correct count
  // and that we get back the correct values
  EXPECT_EQ(tuple_count, table.GetTupleCount());
  oid_t found_tile_group_count = table.GetTileGroupCount();
  oid_t found_tuple_count = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < found_tile_group_count;
       tile_group_itr++) {
    auto tile_group = table.GetTileGroup(tile_group_itr);
    auto tile_count = tile_group->GetTileCount();

    for (oid_t tile_itr = 0; tile_itr < tile_count; tile_itr++) {
      storage::Tile *tile = tile_group->GetTile(tile_itr);
      if (tile == nullptr) continue;
      storage::Tuple tuple(tile->GetSchema());
      storage::TupleIterator tuple_itr(tile);
      while (tuple_itr.Next(tuple)) {
        auto tupleVal = tuple.GetValue(0);
        EXPECT_FALSE(tupleVal.IsNull());

        // I have to do this because we can't put type::Value into a std::set
        bool found = false;
        for (auto val : values) {
          found = val.CompareEquals(tupleVal) == CmpBool::TRUE;
          if (found) break;
        }  // FOR
        EXPECT_TRUE(found);
        // EXPECT_NE(values.end(), values.find(val));

        found_tuple_count++;
      }
    }
  }  // FOR
  EXPECT_EQ(tuple_count, found_tuple_count);
}

}  // namespace test
}  // namespace peloton
