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

#include "executor/executor_tests_util.h"
#include "storage/temp_table.h"
#include "storage/tile_group.h"

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
      {ExecutorTestsUtil::GetColumnInfo(0), ExecutorTestsUtil::GetColumnInfo(1),
       ExecutorTestsUtil::GetColumnInfo(2),
       ExecutorTestsUtil::GetColumnInfo(3)});

  // Create our TempTable
  storage::TempTable table(INVALID_OID, schema, true);

  // Then shove some tuples in it
  for (int i = 0; i < tuple_count; i++) {
    storage::Tuple *tuple = new storage::Tuple(table.GetSchema(), true);
    auto val1 = common::ValueFactory::GetIntegerValue(
        ExecutorTestsUtil::PopulatedValue(i, 0));
    auto val2 = common::ValueFactory::GetIntegerValue(
        ExecutorTestsUtil::PopulatedValue(i, 1));
    auto val3 = common::ValueFactory::GetDoubleValue(
        ExecutorTestsUtil::PopulatedValue(i, 2));
    auto val4 = common::ValueFactory::GetVarcharValue("12345");
    tuple->SetValue(0, val1, pool);
    tuple->SetValue(1, val2, pool);
    tuple->SetValue(2, val3, pool);
    tuple->SetValue(3, val4, pool);

    table.InsertTuple(tuple);
  }

  printf("%s\n", table.GetInfo().c_str());

  // EXPECT_EQ(tuple_count, table.Get)
}

}  // End test namespace
}  // End peloton namespace
