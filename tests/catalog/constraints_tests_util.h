//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraints_tests_util.h
//
// Identification: tests/catalog/constraints_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <memory>

#include "backend/common/types.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

class VarlenPool;

namespace catalog {
class Column;
class Manager;
}

namespace concurrency {
class Transaction;
}

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace storage {
class Backend;
class TileGroup;
class DataTable;
class Tuple;
}

namespace planner {
class ProjectInfo;
}

#define TESTS_TUPLES_PER_TILEGROUP 5
#define DEFAULT_TILEGROUP_COUNT 3

namespace test {

class ConstraintsTestsUtil {
 public:
  /** @brief Creates a basic table with allocated but not populated tuples */
  static storage::DataTable *CreateTable(
      int tuples_per_tilegroup_count = TESTS_TUPLES_PER_TILEGROUP,
      bool indexes = true);

  /** @brief Insert a tupl with 4 columns' value specified */
  static bool ExecuteInsert(concurrency::Transaction *transaction,
                            storage::DataTable *table, const Value &col1,
                            const Value &col2, const Value &col3,
                            const Value &col4);

  /** @brief Creates a basic table with allocated and populated tuples */
  static storage::DataTable *CreateAndPopulateTable();

  static void PopulateTable(concurrency::Transaction *transaction,
                            storage::DataTable *table, int num_rows);

  static catalog::Column GetColumnInfo(int index);

  /**
   * @brief Returns the value populated at the specified field.
   * @param tuple_id Row of the specified field.
   * @param column_id Column of the specified field.
   *
   * This method defines the values that are populated by PopulateTiles().
   *
   * @return Populated value.
   */
  inline static int PopulatedValue(const oid_t tuple_id,
                                   const oid_t column_id) {
    return 10 * tuple_id + column_id;
  }
};

}  // namespace test
}  // namespace peloton
