//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraints_tests_util.cpp
//
// Identification: test/catalog/constraints_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "constraints_tests_util.h"

#include <cstdlib>
#include <ctime>
#include <memory>
#include <vector>

#include "common/harness.h"

#include "catalog/schema.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/exception.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/abstract_executor.h"
#include "executor/logical_tile.h"
#include "storage/tile_group.h"
#include "storage/tile_group_factory.h"
#include "storage/tuple.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"
#include "index/index_factory.h"
#include "planner/delete_plan.h"
#include "planner/insert_plan.h"
#include "executor/executor_context.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"

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
      auto column = catalog::Column(
          type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
          "COL_A", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 1: {
      auto column = catalog::Column(
          type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
          "COL_B", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 2: {
      auto column = catalog::Column(
          type::Type::DECIMAL, type::Type::GetTypeSize(type::Type::DECIMAL),
          "COL_C", is_inlined);

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      return column;
    } break;

    case 3: {
      auto column = catalog::Column(type::Type::VARCHAR,
                                    25,  // Column length.
                                    "COL_D",
                                    !is_inlined);  // inlined.

      column.AddConstraint(catalog::Constraint(ConstraintType::NOTNULL,
                                               not_null_constraint_name));
      column.AddConstraint(
          catalog::Constraint(ConstraintType::UNIQUE, unique_constraint_name));
      return column;
    } break;

    case 114: {
      auto column = catalog::Column(type::Type::INTEGER,
                                    25,  // Column length.
                                    "COL_D",
                                    !is_inlined);  // inlined.

      column.AddConstraint(
          catalog::Constraint(ConstraintType::CHECK, not_null_constraint_name));
      return column;
    }

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
void ConstraintsTestsUtil::PopulateTable(concurrency::Transaction *transaction,
                                         storage::DataTable *table,
                                         int num_rows) {
  // Ensure that the tile group is as expected.
  UNUSED_ATTRIBUTE const catalog::Schema *schema = table->GetSchema();
  PL_ASSERT(schema->GetColumnCount() == 4);

  for (int rowid = 0; rowid < num_rows; rowid++) {
    int populate_value = rowid;

    // First column is unique in this case
    auto col1 =
        type::ValueFactory::GetIntegerValue(PopulatedValue(populate_value, 0));

    // In case of random, make sure this column has duplicated values
    auto col2 =
        type::ValueFactory::GetIntegerValue(PopulatedValue(populate_value, 1));

    auto col3 =
        type::ValueFactory::GetDecimalValue(PopulatedValue(populate_value, 2));

    // In case of random, make sure this column has duplicated values
    auto col4 = type::ValueFactory::GetVarcharValue(
        std::to_string(PopulatedValue(populate_value, 3)));

    ConstraintsTestsUtil::ExecuteInsert(transaction, table, col1, col2, col3,
                                        col4);
  }
}

std::unique_ptr<const planner::ProjectInfo> MakeProjectInfoFromTuple(
    const storage::Tuple *tuple) {
  TargetList target_list;
  DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    std::unique_ptr<type::Value> value(tuple->GetValue(col_id));
    auto expression = expression::ExpressionUtil::ConstantValueFactory(*value);
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
        "primary_btree_index", 123, table->GetOid(), table->GetDatabaseOid(),
        IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, tuple_schema,
        key_schema, key_attrs, unique);

    std::shared_ptr<index::Index> pkey_index(
        index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(pkey_index);

    // SECONDARY INDEX
    key_attrs = {0, 1};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "secondary_btree_index", 124, table->GetOid(), table->GetDatabaseOid(),
        IndexType::BWTREE, IndexConstraintType::DEFAULT, tuple_schema,
        key_schema, key_attrs, unique);
    std::shared_ptr<index::Index> sec_index(
        index::IndexFactory::GetIndex(index_metadata));

    table->AddIndex(sec_index);

    // SECONDARY INDEX - UNIQUE INDEX
    key_attrs = {3};
    key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
    key_schema->SetIndexedColumns(key_attrs);

    unique = false;
    index_metadata = new index::IndexMetadata(
        "unique_btree_index", 125, table->GetOid(), table->GetDatabaseOid(),
        IndexType::BWTREE, IndexConstraintType::UNIQUE, tuple_schema,
        key_schema, key_attrs, unique);
    std::shared_ptr<index::Index> unique_index(
        index::IndexFactory::GetIndex(index_metadata));

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
  txn_manager.CommitTransaction(txn);

  return table;
}
/

}  // namespace test
}  // namespace peloton
