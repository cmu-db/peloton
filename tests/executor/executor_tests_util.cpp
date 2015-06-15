/**
 * @brief Implementation of utility functions for executor tests.
 *
 * Repeated code in many of the executor tests are factored out and placed
 * in this util class.
 *
 * Note that some of the test cases are aware of implementation details
 * of the utility functions i.e. there are implicit contracts between
 * many of the functions here and the test cases. For example, some of the
 * test cases make assumptions about the layout of the tile group returned by
 * CreateSimpleTileGroup().
 *
 * Copyright(c) 2015, CMU
 */

#include "executor/executor_tests_util.h"

#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "catalog/schema.h"
#include "common/value.h"
#include "common/value_factory.h"
#include "common/exception.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tuple.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "index/index_factory.h"

#include "executor/mock_executor.h"
#include "harness.h"

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Return;

namespace nstore {
namespace test {

/** @brief Helper function for defining schema */
catalog::ColumnInfo ExecutorTestsUtil::GetColumnInfo(int index) {
  const bool allow_null = false;
  const bool is_inlined = true;

  switch(index) {
    case 0:
      return catalog::ColumnInfo(
          VALUE_TYPE_INTEGER,
          GetTypeSize(VALUE_TYPE_INTEGER),
          "COL_A",
          allow_null,
          is_inlined);
      break;

    case 1:
      return catalog::ColumnInfo(
          VALUE_TYPE_INTEGER,
          GetTypeSize(VALUE_TYPE_INTEGER),
          "COL_B",
          allow_null,
          is_inlined);
      break;

    case 2:
      return catalog::ColumnInfo(
          VALUE_TYPE_DOUBLE,
          GetTypeSize(VALUE_TYPE_DOUBLE),
          "COL_C",
          allow_null,
          is_inlined);
      break;

    case 3:
      return catalog::ColumnInfo(
          VALUE_TYPE_VARCHAR,
          25, // Column length.
          "COL_D",
          allow_null,
          !is_inlined); // inlined.
      break;

    default:
      throw ExecutorException("Invalid column index : " + std::to_string(index));
      break;
  }
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
storage::TileGroup *ExecutorTestsUtil::CreateTileGroup(
    storage::Backend *backend,
    int tuple_count) {
  std::vector<catalog::ColumnInfo> columns;
  std::vector<catalog::Schema> schemas;

  columns.push_back(GetColumnInfo(0));
  columns.push_back(GetColumnInfo(1));
  catalog::Schema schema1(columns);
  schemas.push_back(schema1);

  columns.clear();
  columns.push_back( GetColumnInfo(2));
  columns.push_back( GetColumnInfo(3));

  catalog::Schema schema2(columns);
  schemas.push_back(schema2);

  storage::TileGroup *tile_group = storage::TileGroupFactory::GetTileGroup(
      INVALID_OID,
      INVALID_OID,
      GetNextTileGroupId(),
      nullptr,
      backend,
      schemas,
      tuple_count);

  return tile_group;
}

/**
 * @brief Populates the table
 * @param table Table to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ExecutorTestsUtil::PopulateTable(
    storage::DataTable *table,
    int num_rows) {

  const catalog::Schema *schema = table->GetSchema();

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  auto& txn_manager = TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  const txn_id_t txn_id = txn->GetTransactionId();

  for (int i = 0; i < num_rows; i++) {
    storage::Tuple tuple(schema, allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(i, 0)));
    tuple.SetValue(1, ValueFactory::GetIntegerValue(PopulatedValue(i, 1)));
    tuple.SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(i, 2)));
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(PopulatedValue(i, 3)));
    tuple.SetValue(3, string_value);

    ItemPointer location = table->InsertTuple(txn_id, &tuple, false);
    EXPECT_TRUE(location.block != INVALID_OID);
    EXPECT_TRUE(location.offset != INVALID_OID);
    txn->RecordInsert(location);

    string_value.FreeUninlinedData();
  }

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
}

/**
 * @brief  Populates the tiles in the given tile-group in a specific manner.
 * @param tile_group Tile-group to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ExecutorTestsUtil::PopulateTiles(
    storage::TileGroup *tile_group,
    int num_rows) {

  // Create tuple schema from tile schemas.
  std::vector<catalog::Schema> &tile_schemas = tile_group->GetTileSchemas();
  std::unique_ptr<catalog::Schema> schema(
      catalog::Schema::AppendSchemaList(tile_schemas));

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  // Insert tuples into tile_group.
  auto& txn_manager = TransactionManager::GetInstance();
  const bool allocate = true;
  auto txn = txn_manager.BeginTransaction();
  const txn_id_t txn_id = txn->GetTransactionId();
  const cid_t commit_id = txn->GetCommitId();

  for (int i = 0; i < num_rows; i++) {
    storage::Tuple tuple(schema.get(), allocate);
    tuple.SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(i, 0)));
    tuple.SetValue(1, ValueFactory::GetIntegerValue(PopulatedValue(i, 1)));
    tuple.SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(i, 2)));
    Value string_value = ValueFactory::GetStringValue(
        std::to_string(PopulatedValue(i, 3)));
    tuple.SetValue(3, string_value);

    oid_t tuple_slot_id = tile_group->InsertTuple(txn_id, &tuple);
    tile_group->CommitInsertedTuple(tuple_slot_id, commit_id);

    string_value.FreeUninlinedData();
  }

  txn_manager.CommitTransaction(txn);
  txn_manager.EndTransaction(txn);
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
  EXPECT_CALL(child_executor, DInit())
  .WillOnce(Return(true));
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

storage::DataTable *ExecutorTestsUtil::CreateTable(int tuples_per_tilegroup_count) {

  catalog::Schema *table_schema = new catalog::Schema( {
    GetColumnInfo(0), GetColumnInfo(1),
        GetColumnInfo(2), GetColumnInfo(3)
  });
  std::string table_name("TEST_TABLE");

  // Create table.
  storage::DataTable *table = storage::TableFactory::GetDataTable(INVALID_OID, table_schema, table_name, tuples_per_tilegroup_count);

  // PRIMARY INDEX
  std::vector<oid_t> key_attrs;
  catalog::Schema *tuple_schema = table->GetSchema();
  catalog::Schema *key_schema;
  index::IndexMetadata *index_metadata;
  bool unique;

  key_attrs = { 0 };
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  unique = true;
  index_metadata = new index::IndexMetadata("PRIMARY_INDEX",
                                            INDEX_TYPE_BTREE_MULTIMAP,
                                            tuple_schema,
                                            key_schema,
                                            unique);
  index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(pkey_index);

  // SECONDARY INDEX
  key_attrs = { 0, 1 };
  key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  unique = false;
  index_metadata = new index::IndexMetadata("SECONDARY_INDEX",
                                            INDEX_TYPE_BTREE_MULTIMAP,
                                            tuple_schema,
                                            key_schema,
                                            unique);
  index::Index *sec_index = index::IndexFactory::GetInstance(index_metadata);

  table->AddIndex(sec_index);

  return table;
}

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *ExecutorTestsUtil::CreateAndPopulateTable() {

  const int tuple_count = 50;
  storage::DataTable *table = ExecutorTestsUtil::CreateTable(tuple_count);
  ExecutorTestsUtil::PopulateTable(table, tuple_count * 3);

  return table;
}

storage::Tuple *ExecutorTestsUtil::GetTuple(storage::DataTable *table, oid_t tuple_id) {

  storage::Tuple* tuple = new storage::Tuple(table->GetSchema(), true);
  tuple->SetValue(0, ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 0)));
  tuple->SetValue(1, ValueFactory::GetIntegerValue(PopulatedValue(tuple_id, 1)));
  tuple->SetValue(2, ValueFactory::GetDoubleValue(PopulatedValue(tuple_id, 2)));
  tuple->SetValue(3, ValueFactory::GetStringValue("12345"));

  return tuple;
}

storage::Tuple *ExecutorTestsUtil::GetNullTuple(storage::DataTable *table) {

  storage::Tuple* tuple = new storage::Tuple(table->GetSchema(), true);
  tuple->SetValue(0, ValueFactory::GetNullValue());
  tuple->SetValue(1, ValueFactory::GetNullValue());
  tuple->SetValue(2, ValueFactory::GetNullValue());
  tuple->SetValue(3, ValueFactory::GetNullStringValue());

  return tuple;
}

} // namespace test
} // namespace nstore
