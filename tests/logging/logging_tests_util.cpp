//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// checkpoint_test.cpp
//
// Identification: tests/logging/checkpoint_test.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "logging/logging_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// LoggingTests Util
//===--------------------------------------------------------------------===//

std::vector<logging::TupleRecord> LoggingTestsUtil::BuildTupleRecords(
    std::vector<std::shared_ptr<storage::Tuple>> &tuples,
    size_t tile_group_size, size_t table_tile_group_count) {
  std::vector<logging::TupleRecord> records;
  for (size_t block = 1; block <= table_tile_group_count; ++block) {
    for (size_t offset = 0; offset < tile_group_size; ++offset) {
      ItemPointer location(block, offset);
      auto &tuple = tuples[(block - 1) * tile_group_size + offset];
      assert(tuple->GetSchema());
      logging::TupleRecord record(
          LOGRECORD_TYPE_WAL_TUPLE_INSERT, INITIAL_TXN_ID, INVALID_OID,
          location, INVALID_ITEMPOINTER, tuple.get(), DEFAULT_DB_ID);
      record.SetTuple(tuple.get());
      records.push_back(record);
    }
  }
  LOG_INFO("Built a vector of %lu tuple WAL insert records", records.size());
  return records;
}

std::vector<std::shared_ptr<storage::Tuple>> LoggingTestsUtil::BuildTuples(
    storage::DataTable *table, int num_rows, bool mutate, bool random) {
  std::vector<std::shared_ptr<storage::Tuple>> tuples;
  LOG_INFO("build a vector of %d tuples", num_rows);

  // Random values
  std::srand(std::time(nullptr));
  const catalog::Schema *schema = table->GetSchema();
  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    std::shared_ptr<storage::Tuple> tuple(new storage::Tuple(schema, allocate));

    // First column is unique in this case
    tuple->SetValue(0,
                    ValueFactory::GetIntegerValue(
                        ExecutorTestsUtil::PopulatedValue(populate_value, 0)),
                    testing_pool);

    // In case of random, make sure this column has duplicated values
    tuple->SetValue(
        1, ValueFactory::GetIntegerValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() % (num_rows / 3) : populate_value, 1)),
        testing_pool);

    tuple->SetValue(
        2, ValueFactory::GetDoubleValue(ExecutorTestsUtil::PopulatedValue(
               random ? std::rand() : populate_value, 2)),
        testing_pool);

    // In case of random, make sure this column has duplicated values
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(ExecutorTestsUtil::PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple->SetValue(3, string_value, testing_pool);
    assert(tuple->GetSchema());
    tuples.push_back(std::move(tuple));
  }
  return tuples;
}

}  // End test namespace
}  // End peloton namespace
