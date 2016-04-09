//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraints_tests_util.cpp
//
// Identification: tests/catalog/constraints_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/constraints_tests_util.h"

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
#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/executor/executor_context.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/expression/expression_util.h"

#include "executor/mock_executor.h"

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

/** @brief Helper function for defining schema */
catalog::Column ConstraintsTestsUtil::GetColumnInfo(int index) {
  const bool is_inlined = true;
  std::string not_null_constraint_name = "not_null";
  std::string unique_constraint_name = "unique";
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
      column.AddConstraint(
          catalog::Constraint(CONSTRAINT_TYPE_UNIQUE, unique_constraint_name));
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
 * @brief Populates the table
 * @param table Table to populate with values.
 * @param num_rows Number of tuples to insert.
 */
void ConstraintsTestsUtil::PopulateTable(__attribute__((unused))
                                         concurrency::Transaction *transaction,
                                         storage::DataTable *table,
                                         int num_rows) {
  const catalog::Schema *schema = table->GetSchema();

  // Ensure that the tile group is as expected.
  assert(schema->GetColumnCount() == 4);

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;

    Value col1, col2, col3, col4;

    // First column is unique in this case
    col1 = ValueFactory::GetIntegerValue(PopulatedValue(populate_value, 0));

    // In case of random, make sure this column has duplicated values
    col2 = ValueFactory::GetIntegerValue(PopulatedValue(populate_value, 1));

    col3 = ValueFactory::GetDoubleValue(PopulatedValue(populate_value, 2));

    // In case of random, make sure this column has duplicated values
    col4 = ValueFactory::GetStringValue(
        std::to_string(PopulatedValue(populate_value, 3)));

    ConstraintsTestsUtil::ExecuteInsert(transaction, table, col1, col2, col3,
                                        col4);
  }
}

std::unique_ptr<const planner::ProjectInfo> MakeProjectInfoFromTuple(
    const storage::Tuple *tuple) {
  planner::ProjectInfo::TargetList target_list;
  planner::ProjectInfo::DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    auto value = tuple->GetValue(col_id);
    auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
    target_list.emplace_back(col_id, expression);
  }

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

bool ConstraintsTestsUtil::ExecuteInsert(concurrency::Transaction *transaction,
                                         storage::DataTable *table,
                                         const Value &col1, const Value &col2,
                                         const Value &col3, const Value &col4) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Make tuple
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));

  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  tuple->SetValue(0, col1, testing_pool);
  tuple->SetValue(1, col2, testing_pool);
  tuple->SetValue(2, col3, testing_pool);
  tuple->SetValue(3, col4, testing_pool);

  auto project_info = MakeProjectInfoFromTuple(tuple.get());

  // Insert
  planner::InsertPlan node(table, std::move(project_info));
  executor::InsertExecutor executor(&node, context.get());
  return executor.Execute();
}

storage::DataTable *ConstraintsTestsUtil::CreateTable(
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

    // SECONDARY INDEX - UNIQUE INDEX
    key_attrs = {3};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "unique_btree_index", 125, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_UNIQUE, tuple_schema, key_schema, unique);
    index::Index *unique_index =
        index::IndexFactory::GetInstance(index_metadata);

    table->AddIndex(unique_index);
  }

  return table;
}

/**
 * @brief Convenience method to create table for test.
 *
 * @return Table generated for test.
 */
storage::DataTable *ConstraintsTestsUtil::CreateAndPopulateTable() {
  const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
  storage::DataTable *table = ConstraintsTestsUtil::CreateTable(tuple_count);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  ConstraintsTestsUtil::PopulateTable(txn, table,
                                      tuple_count * DEFAULT_TILEGROUP_COUNT);
  txn_manager.CommitTransaction();

  return table;
}

}  // namespace test
}  // namespace peloton
