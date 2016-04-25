//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_tests_util.cpp
//
// Identification: tests/executor/executor_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/executor_tests_util.h"

#include <cstdlib>
#include <ctime>
#include <memory>
#include <vector>

#include "harness.h"

#include "backend/catalog/schema.h"
#include "backend/common/value.h"
#include "backend/common/value_factory.h"
#include "backend/common/exception.h"
#include "backend/concurrency/transaction.h"
#include "backend/concurrency/transaction_manager_factory.h"
#include "backend/executor/abstract_executor.h"
#include "backend/executor/logical_tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/tile_group_factory.h"
#include "backend/storage/tuple.h"
#include "backend/storage/data_table.h"
#include "backend/storage/table_factory.h"
#include "backend/index/index_factory.h"

#include "executor/mock_executor.h"

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

/** @brief Helper function for defining schema */
catalog::Column ExecutorTestsUtil::GetColumnInfo(int index) {
  const bool is_inlined = true;
  std::string not_null_constraint_name = "not_null";
  catalog::Column dummy_column;

  switch (index) {
    case 0: {
      auto column =
          catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "COL_A", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 1: {
      auto column =
          catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "COL_B", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 2: {
      auto column =
          catalog::Column(VALUE_TYPE_DOUBLE, GetTypeSize(VALUE_TYPE_DOUBLE),
                          "COL_C", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 3: {
      auto column = catalog::Column(VALUE_TYPE_VARCHAR,
                                    25,  // Column length.
                                    "COL_D",
                                    !is_inlined);  // inlined.

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    default: {
      throw ExecutorException("Invalid column index : " +
                              std::to_string(index));
    }
  }

  return dummy_column;
}

/**
 * @brief Creates simple tile group for testing purposes.
 * @param backend Backend for tile group to use.
 * @param tuple_count Tuple capacity of this tile group.
 *
 * Tile group has two tiles, and each of them has two columns.
 * The first two columns have INTEGER types, the last two have TINYINT
 * and VARCHAR.
 *
 * IMPORTANT: If you modify this function, it is your responsibility to
 *            fix any affected test cases. Test cases may be depending
 *            on things like the specific number of tiles in this group.
 *
 * @return Pointer to tile group.
 */
std::shared_ptr<storage::TileGroup> ExecutorTestsUtil::CreateTileGroup(
    int tuple_count) {
  std::vector<catalog::Column> columns;
  std::vector<catalog::Schema> schemas;

  columns.push_back(GetColumnInfo(0));
  columns.push_back(GetColumnInfo(1));
  catalog::Schema schema1(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back(GetColumnInfo(2));
  columns.push_back(GetColumnInfo(3));

  catalog::Schema schema2(columns);
  schemas.push_back(schema2);

  std::map<oid_t, std::pair<oid_t, oid_t>> column_map;
  column_map[0] = std::make_pair(0, 0);
  column_map[1] = std::make_pair(0, 1);
  column_map[2] = std::make_pair(1, 0);
  column_map[3] = std::make_pair(1, 1);

  std::shared_ptr<storage::TileGroup> tile_group_ptr(
      storage::TileGroupFactory::GetTileGroup(
          INVALID_OID, INVALID_OID,
          TestingHarness::GetInstance().GetNextTileGroupId(), nullptr, schemas,
          column_map, tuple_count));

  catalog::Manager::GetInstance().AddTileGroup(tile_group_ptr->GetTileGroupId(),
                                               tile_group_ptr);
  return tile_group_ptr;
}

/**
 * @brief Populates the table
 * @param table Table to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ExecutorTestsUtil::PopulateTable(storage::DataTable *table, int num_rows,
                                      bool mutate, bool random, bool group_by) {
  // Random values
  if (random) std::srand(std::time(nullptr));

  const catalog::Schema *schema = table->GetSchema();

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    storage::Tuple tuple(schema, allocate);

    if (group_by) {
      // First column has only two distinct values
      tuple.SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(
          int(populate_value / (num_rows / 2)), 0)),
                     testing_pool);

    } else {
      // First column is unique in this case
      tuple.SetValue(
          0, ValueFactory::GetIntegerValue(PopulatedValue(populate_value, 0)),
          testing_pool);
    }

    // In case of random, make sure this column has duplicated values
    tuple.SetValue(
        1, ValueFactory::GetIntegerValue(PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 1)),
            testing_pool);

    tuple.SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(
        random ? std::rand() : populate_value, 2)),
                   testing_pool);

    // In case of random, make sure this column has duplicated values
    Value string_value =
        ValueFactory::GetStringValue(std::to_string(PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    ItemPointer tuple_slot_id = table->InsertTuple(&tuple);
    EXPECT_TRUE(tuple_slot_id.block != INVALID_OID);
    EXPECT_TRUE(tuple_slot_id.offset != INVALID_OID);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.PerformInsert(tuple_slot_id);
  }
}

/**
 * @brief  Populates the tiles in the given tile-group in a specific manner.
 * @param tile_group Tile-group to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ExecutorTestsUtil::PopulateTiles(
    std::shared_ptr<storage::TileGroup> tile_group, int num_rows) {
  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  txn_manager.BeginTransaction();
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int col_itr = 0; col_itr < num_rows; col_itr++) {
    storage::Tuple tuple(schema.get(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(col_itr, 0)),
                   testing_pool);
    tuple.SetValue(1, ValueFactory::GetIntegerValue(PopulatedValue(col_itr, 1)),
                   testing_pool);
    tuple.SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(col_itr, 2)),
                   testing_pool);
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(PopulatedValue(col_itr, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    oid_t tuple_slot_id = tile_group->InsertTuple(&tuple);
    txn_manager.PerformInsert(ItemPointer(tile_group->GetTileGroupId(), tuple_slot_id));
  }

  txn_manager.CommitTransaction();
}

/**
 * @brief Convenience function to pass a single logical tile through an
 *        executor which has only one child.
 * @param executor Executor to pass logical tile through.
 * @param source_logical_tile Logical tile to pass through executor.
 * @param check the value of logical tiles
 *
 * @return Pointer to processed logical tile.
 */
executor::LogicalTile *ExecutorTestsUtil::ExecuteTile(
    executor::AbstractExecutor *executor,
    executor::LogicalTile *source_logical_tile) {
  MockExecutor child_executor;
  executor->AddChild(&child_executor);

  // Uneventful init...
  EXPECT_CALL(child_executor, DInit()).WillOnce(Return(true));
  EXPECT_TRUE(executor->Init());

  // Where the main work takes place...
  EXPECT_CALL(child_executor, DExecute())
  .WillOnce(Return(true))
  .WillOnce(Return(false));

  EXPECT_CALL(child_executor, GetOutput())
  .WillOnce(Return(source_logical_tile));

  EXPECT_TRUE(executor->Execute());
  std::unique_ptr<executor::LogicalTile> result_logical_tile(
      executor->GetOutput());
  EXPECT_THAT(result_logical_tile, NotNull());
  EXPECT_THAT(executor->Execute(), false);

  return result_logical_tile.release();
}

storage::DataTable *ExecutorTestsUtil::CreateTable(
    int tuples_per_tilegroup_count, bool indexes) {
  catalog::Schema *table_schema = new catalog::Schema(
      {GetColumnInfo(0), GetColumnInfo(1), GetColumnInfo(2), GetColumnInfo(3)});
  std::string table_name("TEST_TABLE");

  // Create table.
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name,
      tuples_per_tilegroup_count, own_schema, adapt_table);

  if (indexes == true) {
    // PRIMARY INDEX
    std::vector<oid_t> key_attrs;

    auto tuple_schema = table->GetSchema();
    catalog::Schema *key_schema;
    index::IndexMetadata *index_metadata;
    bool unique;

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = true;

    index_metadata = new index::IndexMetadata(
        "primary_btree_index", 123, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

    index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

    table->AddIndex(pkey_index);

    // SECONDARY INDEX
    key_attrs = {0, 1};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "secondary_btree_index", 124, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_DEFAULT, tuple_schema, key_schema, unique);
    index::Index *sec_index = index::IndexFactory::GetInstance(index_metadata);

    table->AddIndex(sec_index);
  }

  return table;
}

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *ExecutorTestsUtil::CreateAndPopulateTable() {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  storage::DataTable *table = ExecutorTestsUtil::CreateTable(tuple_count);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  txn_manager.BeginTransaction();
  ExecutorTestsUtil::PopulateTable(
      table, tuple_count * DEFAULT_TILEGROUP_COUNT, false, false, false);
  txn_manager.CommitTransaction();

  return table;
}

std::unique_ptr<storage::Tuple> ExecutorTestsUtil::GetTuple(storage::DataTable *table,
                                                            oid_t tuple_id, VarlenPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(), true));
  tuple->SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 0)),
                  pool);
  tuple->SetValue(1, ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 1)),
                  pool);
  tuple->SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(tuple_id, 2)),
                  pool);
  tuple->SetValue(3, ValueFactory::GetStringValue("12345"), pool);

  return tuple;
}

std::unique_ptr<storage::Tuple> ExecutorTestsUtil::GetNullTuple(storage::DataTable *table,
                                                                VarlenPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(), true));
  tuple->SetValue(0, ValueFactory::GetNullValue(), pool);
  tuple->SetValue(1, ValueFactory::GetNullValue(), pool);
  tuple->SetValue(2, ValueFactory::GetNullValue(), pool);
  tuple->SetValue(3, ValueFactory::GetNullStringValue(), pool);

  return tuple;
}

void ExecutorTestsUtil::PrintTileVector(
    std::vector<std::unique_ptr<executor::LogicalTile>> &tile_vec) {
  for (auto &tile : tile_vec) {
    for (oid_t tuple_id : *tile) {
      LOG_INFO("<");
      for (oid_t col_id = 0; col_id < tile->GetColumnCount(); col_id++) {
        LOG_INFO("%s", tile->GetValue(tuple_id, col_id).GetInfo().c_str());
      }
      LOG_INFO(">");
    }
  }
}

}  // namespace test
}  // namespace peloton
