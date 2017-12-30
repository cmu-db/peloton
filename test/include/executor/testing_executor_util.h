//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_tests_util.h
//
// Identification: test/include/executor/executor_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "common/internal_types.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

namespace catalog {
class Column;
class Manager;
}

namespace concurrency {
class TransactionContext;
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
class Database;
}

namespace type {
class AbstractPool;
};

#define TESTS_TUPLES_PER_TILEGROUP 5
#define DEFAULT_TILEGROUP_COUNT 3

namespace test {

class TestingExecutorUtil {
 public:
  /**
   * @brief Intializes the catalog with a new database with the give name.
   */
  static storage::Database *InitializeDatabase(const std::string &db_name);

  static void DeleteDatabase(const std::string &db_name);

  /**
   * @brief Creates a basic tile group with allocated but not populated
   *        tuples.
   */
  static std::shared_ptr<storage::TileGroup> CreateTileGroup(
      int allocate_tuple_count = TESTS_TUPLES_PER_TILEGROUP);

  /** @brief Creates a basic table with allocated but not populated tuples */
  static storage::DataTable *CreateTable(
      int tuples_per_tilegroup_count = TESTS_TUPLES_PER_TILEGROUP,
      bool indexes = true, oid_t table_oid = INVALID_OID);

  /** @brief Creates a basic table with allocated and populated tuples */
  static storage::DataTable *CreateAndPopulateTable();

  static void PopulateTable(storage::DataTable *table, int num_rows,
                            bool mutate, bool random, bool group_by,
                            concurrency::TransactionContext *current_txn);

  static void PopulateTiles(std::shared_ptr<storage::TileGroup> tile_group,
                            int num_rows);

  static catalog::Column GetColumnInfo(int index);

  static executor::LogicalTile *ExecuteTile(
      executor::AbstractExecutor *executor,
      executor::LogicalTile *source_logical_tile);

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

  static std::unique_ptr<storage::Tuple> GetTuple(storage::DataTable *table,
                                                  oid_t tuple_id,
                                                  type::AbstractPool *pool);
  static std::unique_ptr<storage::Tuple> GetNullTuple(storage::DataTable *table,
                                                      type::AbstractPool *pool);

  /** Print the tuples from a vector of logical tiles */
  static std::string GetTileVectorInfo(
      std::vector<std::unique_ptr<executor::LogicalTile>> &tile_vec);
};

}  // namespace test
}  // namespace peloton
