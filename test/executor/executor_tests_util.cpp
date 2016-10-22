//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// executor_tests_util.cpp
//
// Identification: test/executor/executor_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "executor/executor_tests_util.h"

#include <cstdlib>
#include <ctime>
#include <memory>
#include <vector>

#include "common/harness.h"

#include "catalog/schema.h"
#include "common/exception.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "index/index_factory.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tuple.h"

#include "executor/mock_executor.h"

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

/** @brief Helper function for defining schema */
/*
 * GetColumnInfo() - Returns column object for testing
 *
 * Column 0: Integer column, not null
 * Column 1: Integer column, not null
 * Column 2: Double column, not null
 * Column 3: VARCHAR, max len = 25, not null
 *
 * For other column IDs an exception will be thrown
 */
catalog::Column ExecutorTestsUtil::GetColumnInfo(int index) {
  const bool is_inlined = true;
  std::string not_null_constraint_name = "not_null";
  catalog::Column dummy_column;

  switch (index) {
    case 0: {
      auto column =
          catalog::Column(common::Type::INTEGER,
                          common::Type::GetTypeSize(common::Type::INTEGER),
                          "COL_A", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 1: {
      auto column =
          catalog::Column(common::Type::INTEGER,
                          common::Type::GetTypeSize(common::Type::INTEGER),
                          "COL_B", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 2: {
      auto column =
          catalog::Column(common::Type::DECIMAL,
                          common::Type::GetTypeSize(common::Type::DECIMAL),
                          "COL_C", is_inlined);

      column.AddConstraint(catalog::Constraint(CONSTRAINT_TYPE_NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 3: {
      auto column =
          catalog::Column(common::Type::VARCHAR, 25,  // Column length.
                          "COL_D", !is_inlined);      // inlined.

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
                                      bool mutate, bool random, bool group_by,
                                      concurrency::Transaction *current_txn) {
  // Random values
  if (random) std::srand(std::time(nullptr));

  const catalog::Schema *schema = table->GetSchema();

  // Ensure that the tile group is as expected.
  PL_ASSERT(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  const bool allocate = true;
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;
    if (mutate) populate_value *= 3;

    storage::Tuple tuple(schema, allocate);

    if (group_by) {
      // First column has only two distinct values
      tuple.SetValue(0, common::ValueFactory::GetIntegerValue(PopulatedValue(
                            int(populate_value / (num_rows / 2)), 0)),
                     testing_pool);

    } else {
      // First column is unique in this case
      tuple.SetValue(0, common::ValueFactory::GetIntegerValue(
                            PopulatedValue(populate_value, 0)),
                     testing_pool);
    }

    // In case of random, make sure this column has duplicated values
    tuple.SetValue(
        1, common::ValueFactory::GetIntegerValue(PopulatedValue(
               random ? std::rand() % (num_rows / 3) : populate_value, 1)),
        testing_pool);

    tuple.SetValue(2, common::ValueFactory::GetDoubleValue(PopulatedValue(
                          random ? std::rand() : populate_value, 2)),
                   testing_pool);

    // In case of random, make sure this column has duplicated values
    auto string_value =
        common::ValueFactory::GetVarcharValue(std::to_string(PopulatedValue(
            random ? std::rand() % (num_rows / 3) : populate_value, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    ItemPointer *index_entry_ptr = nullptr;
    ItemPointer tuple_slot_id =
        table->InsertTuple(&tuple, current_txn, &index_entry_ptr);
    PL_ASSERT(tuple_slot_id.block != INVALID_OID);
    PL_ASSERT(tuple_slot_id.offset != INVALID_OID);

    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    txn_manager.PerformInsert(current_txn, tuple_slot_id, index_entry_ptr);
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
  PL_ASSERT(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  const bool allocate = true;
  auto current_txn = txn_manager.BeginTransaction();
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();

  for (int col_itr = 0; col_itr < num_rows; col_itr++) {
    storage::Tuple tuple(schema.get(), allocate);
    tuple.SetValue(
        0, common::ValueFactory::GetIntegerValue(PopulatedValue(col_itr, 0)),
        testing_pool);
    tuple.SetValue(
        1, common::ValueFactory::GetIntegerValue(PopulatedValue(col_itr, 1)),
        testing_pool);
    tuple.SetValue(
        2, common::ValueFactory::GetDoubleValue(PopulatedValue(col_itr, 2)),
        testing_pool);
    auto string_value = common::ValueFactory::GetVarcharValue(
        std::to_string(PopulatedValue(col_itr, 3)));
    tuple.SetValue(3, string_value, testing_pool);

    ItemPointer *index_entry_ptr = nullptr;
    oid_t tuple_slot_id = tile_group->InsertTuple(&tuple);
    txn_manager.PerformInsert(
        current_txn, ItemPointer(tile_group->GetTileGroupId(), tuple_slot_id),
        index_entry_ptr);
  }

  txn_manager.CommitTransaction(current_txn);
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
    int tuples_per_tilegroup_count, bool indexes, oid_t table_oid) {
  catalog::Schema *table_schema = new catalog::Schema(
      {GetColumnInfo(0), GetColumnInfo(1), GetColumnInfo(2), GetColumnInfo(3)});
  std::string table_name("TEST_TABLE");

  // Create table.
  bool own_schema = true;
  bool adapt_table = false;
  storage::DataTable *table = storage::TableFactory::GetDataTable(
      INVALID_OID, table_oid, table_schema, table_name,
      tuples_per_tilegroup_count, own_schema, adapt_table);

  if (indexes == true) {
    // This holds column ID in the underlying table that are being indexed
    std::vector<oid_t> key_attrs;

    // This holds schema of the underlying table, which stays all the same
    // for all indices on the same underlying table
    auto tuple_schema = table->GetSchema();

    // This points to the schmea of only columns indiced by the index
    // This is basically selecting tuple_schema() with key_attrs as index
    // but the order inside tuple schema is preserved - the order of schema
    // inside key_schema is not the order of real key
    catalog::Schema *key_schema;

    // This will be created for each index on the table
    // and the metadata is passed as part of the index construction paratemter
    // list
    index::IndexMetadata *index_metadata;

    // Whether keys should be unique. For primary key this must be true;
    // for secondary keys this might be true as an extra constraint
    bool unique;

    /////////////////////////////////////////////////////////////////
    // Add primary key on column 0
    /////////////////////////////////////////////////////////////////

    key_attrs = {0};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);

    // This is not redundant
    // since the key schema always follows the ordering of the base table
    // schema, we need real ordering of the key columns
    key_schema->SetIndexedColumns(key_attrs);

    unique = true;

    index_metadata = new index::IndexMetadata(
        "primary_btree_index", 123, INVALID_OID, INVALID_OID, INDEX_TYPE_BWTREE,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
        unique);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetInstance(index_metadata));

    table->AddIndex(pkey_index);

    /////////////////////////////////////////////////////////////////
    // Add index on table column 0 and 1
    /////////////////////////////////////////////////////////////////

    key_attrs = {0, 1};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "secondary_btree_index", 124, INVALID_OID, INVALID_OID,
        INDEX_TYPE_BWTREE, INDEX_CONSTRAINT_TYPE_DEFAULT, tuple_schema,
        key_schema, key_attrs, unique);
    std::shared_ptr<index::Index> sec_index(
        index::IndexFactory::GetInstance(index_metadata));

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
  auto txn = txn_manager.BeginTransaction();
  ExecutorTestsUtil::PopulateTable(table, tuple_count * DEFAULT_TILEGROUP_COUNT,
                                   false, false, false, txn);
  txn_manager.CommitTransaction(txn);

  return table;
}

std::unique_ptr<storage::Tuple> ExecutorTestsUtil::GetTuple(
    storage::DataTable *table, oid_t tuple_id, common::VarlenPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto val1 =
      common::ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 0));
  auto val2 =
      common::ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 1));
  auto val3 = common::ValueFactory::GetDoubleValue(PopulatedValue(tuple_id, 2));
  auto val4 = common::ValueFactory::GetVarcharValue("12345");
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);

  return tuple;
}

std::unique_ptr<storage::Tuple> ExecutorTestsUtil::GetNullTuple(
    storage::DataTable *table, common::VarlenPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto val1 = common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
  auto val2 = common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
  auto val3 = common::ValueFactory::GetNullValueByType(common::Type::INTEGER);
  auto val4 = common::ValueFactory::GetNullValueByType(common::Type::VARCHAR);
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);

  return tuple;
}

void ExecutorTestsUtil::PrintTileVector(
    std::vector<std::unique_ptr<executor::LogicalTile>> &tile_vec) {
  for (auto &tile : tile_vec) {
    for (UNUSED_ATTRIBUTE oid_t tuple_id : *tile) {
      LOG_INFO("<");
      for (oid_t col_id = 0; col_id < tile->GetColumnCount(); col_id++) {
        LOG_INFO("%s",
                           tile->GetValue(tuple_id, col_id).GetInfo()
                           .c_str());
      }
      LOG_INFO(">");
    }
  }
}

}  // namespace test
}  // namespace peloton
