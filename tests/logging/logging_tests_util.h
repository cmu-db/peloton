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

#include "harness.h"
#include "backend/logging/records/tuple_record.h"
#include "backend/storage/data_table.h"
#include "backend/storage/tile.h"
#include "executor/executor_tests_util.h"

#define DEFAULT_RECOVERY_CID 15

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// LoggingTests Utility
//===--------------------------------------------------------------------===//
class LoggingTestsUtil {
 public:
  static std::vector<logging::TupleRecord> BuildTupleRecords(
      std::vector<std::shared_ptr<storage::Tuple>> &tuples,
      size_t tile_group_size, size_t table_tile_group_count);

  static std::vector<std::shared_ptr<storage::Tuple>> BuildTuples(
      storage::DataTable *table, int num_rows, bool mutate, bool random);
};

}  // End test namespace
}  // End peloton namespace
