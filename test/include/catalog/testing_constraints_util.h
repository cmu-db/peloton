//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// constraints_tests_util.h
//
// Identification: test/include/catalog/constraints_tests_util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <memory>
#include <cstdlib>
#include <ctime>

#include "common/harness.h"
#include "executor/mock_executor.h"

#include "common/internal_types.h"
#include "catalog/catalog.h"
#include "catalog/database_catalog.h"
#include "catalog/schema.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "common/exception.h"
#include "concurrency/transaction_context.h"
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
#include "planner/project_info.h"
#include "executor/executor_context.h"
#include "executor/delete_executor.h"
#include "executor/insert_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"

using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Return;

namespace peloton {

//===--------------------------------------------------------------------===//
// Utils
//===--------------------------------------------------------------------===//

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

#define CONSTRAINTS_TEST_TABLE "test_table"
#define TESTS_TUPLES_PER_TILEGROUP 5
#define DEFAULT_TILEGROUP_COUNT 3
#define CONSTRAINTS_NUM_COLS 4

namespace test {

class TestingConstraintsUtil {
 public:

  /** @brief Creates a basic table with allocated and populated tuples */
  static storage::DataTable *CreateAndPopulateTable(
      std::vector<std::vector<catalog::Constraint>> constraints,
      std::vector<catalog::MultiConstraint> multi_constraints) {
    const int tuple_count = TESTS_TUPLES_PER_TILEGROUP;
    storage::DataTable *table = TestingConstraintsUtil::CreateTable(
        constraints, multi_constraints);
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    TestingConstraintsUtil::PopulateTable(txn, table,
                                          tuple_count * DEFAULT_TILEGROUP_COUNT);
    txn_manager.CommitTransaction(txn);

    return table;
  };

  /** @brief Creates a basic table with allocated but not populated tuples */
  static storage::DataTable *CreateTable(
      std::vector<std::vector<catalog::Constraint>> constraints,
      UNUSED_ATTRIBUTE std::vector<catalog::MultiConstraint> multi_constraints,
      UNUSED_ATTRIBUTE bool indexes = true) {

    // Create the database
    auto catalog = catalog::Catalog::GetInstance();
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto txn = txn_manager.BeginTransaction();
    catalog->CreateDatabase(DEFAULT_DB_NAME, txn);
    txn_manager.CommitTransaction(txn);

    // First populate the list of catalog::Columns that we
    // are going to need for this test
    std::vector<catalog::Column> columns;
    for (int i = 0; i < CONSTRAINTS_NUM_COLS; i++) {
      columns.push_back(TestingConstraintsUtil::GetColumnInfo(i, constraints[i]));
    }
    std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema(columns));
    std::string table_name(CONSTRAINTS_TEST_TABLE);

    // Create table.
    txn = txn_manager.BeginTransaction();
    auto result = catalog->CreateTable(DEFAULT_DB_NAME,
                                       table_name,
                                       std::move(table_schema),
                                       txn,
                                       false);
    txn_manager.CommitTransaction(txn);
    EXPECT_EQ(ResultType::SUCCESS, result);

    txn = txn_manager.BeginTransaction();
    auto db = catalog->GetDatabaseWithName(DEFAULT_DB_NAME, txn);
    storage::DataTable *table = db->GetTableWithName(table_name);
    txn_manager.CommitTransaction(txn);
    EXPECT_NE(nullptr, table);

//    if (indexes == true) {
//      // PRIMARY INDEX
//      std::vector<oid_t> key_attrs;
//
//      auto tuple_schema = table->GetSchema();
//      catalog::Schema *key_schema;
//      index::IndexMetadata *index_metadata;
//      bool unique;
//
//      key_attrs = {0};
//      key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
//      key_schema->SetIndexedColumns(key_attrs);
//
//      unique = true;
//
//      index_metadata = new index::IndexMetadata(
//          "primary_btree_index", 123, table->GetOid(), table->GetDatabaseOid(),
//          IndexType::BWTREE, IndexConstraintType::PRIMARY_KEY, tuple_schema,
//          key_schema, key_attrs, unique);
//
//      std::shared_ptr<index::Index> pkey_index(
//          index::IndexFactory::GetIndex(index_metadata));
//
//      table->AddIndex(pkey_index);
//
//      // SECONDARY INDEX
//      key_attrs = {0, 1};
//      key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
//      key_schema->SetIndexedColumns(key_attrs);
//
//      unique = false;
//      index_metadata = new index::IndexMetadata(
//          "secondary_btree_index", 124, table->GetOid(),
//          table->GetDatabaseOid(), IndexType::BWTREE,
//          IndexConstraintType::DEFAULT, tuple_schema, key_schema, key_attrs,
//          unique);
//      std::shared_ptr<index::Index> sec_index(
//          index::IndexFactory::GetIndex(index_metadata));
//
//      table->AddIndex(sec_index);
//
//      // SECONDARY INDEX - UNIQUE INDEX
//      key_attrs = {3};
//      key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
//      key_schema->SetIndexedColumns(key_attrs);
//
//      unique = false;
//      index_metadata = new index::IndexMetadata(
//          "unique_btree_index", 125, table->GetOid(), table->GetDatabaseOid(),
//          IndexType::BWTREE, IndexConstraintType::UNIQUE, tuple_schema,
//          key_schema, key_attrs, unique);
//      std::shared_ptr<index::Index> unique_index(
//          index::IndexFactory::GetIndex(index_metadata));
//
//      table->AddIndex(unique_index);
//    }

    return table;
  };

  static std::unique_ptr<const planner::ProjectInfo> MakeProjectInfoFromTuple(
      const storage::Tuple *tuple) {
    TargetList target_list;
    DirectMapList direct_map_list;

    for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
      std::unique_ptr<type::Value> value(
          new type::Value(tuple->GetValue(col_id)));
      auto expression =
          expression::ExpressionUtil::ConstantValueFactory(*value);
      planner::DerivedAttribute attribute(expression);
      target_list.emplace_back(col_id, attribute);
    }

    return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
        std::move(target_list), std::move(direct_map_list)));
  };

  /** @brief Insert a tuple with 1 columns' value specified */
  static bool ExecuteOneInsert(concurrency::TransactionContext *transaction,
                               storage::DataTable *table,
                               const type::Value &col1) {
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(transaction));

    // Make tuple
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table->GetSchema(), true));

    auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
    tuple->SetValue(0, col1, testing_pool);
    auto project_info = MakeProjectInfoFromTuple(tuple.get());

    // Insert
    planner::InsertPlan node(table, std::move(project_info));
    executor::InsertExecutor executor(&node, context.get());
    return executor.Execute();
  };

  /** @brief Delete all tuples from a table */
//  static bool ExecuteTruncate(concurrency::TransactionContext *transaction,
//                              storage::DataTable *table) {
//    std::unique_ptr<executor::ExecutorContext> context(
//        new executor::ExecutorContext(transaction));
//    planner::DeletePlan node(table, true);
//    executor::DeleteExecutor executor(&node, context.get());
//    executor.Init();
//    return executor.Execute();
//  };

  /** @brief Insert a tuple with 1 columns' value specified */
  static bool ExecuteMultiInsert(concurrency::TransactionContext *transaction,
                                 storage::DataTable *table,
                                 std::vector<type::Value> cols) {
    std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(transaction));

    // Make tuple
    std::unique_ptr<storage::Tuple> tuple(
        new storage::Tuple(table->GetSchema(), true));

    auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
    for (int i = 0; i < int(cols.size()); i++) {
      tuple->SetValue(i, cols[i], testing_pool);
    }

    auto project_info = MakeProjectInfoFromTuple(tuple.get());
    // Insert
    planner::InsertPlan node(table, std::move(project_info));
    executor::InsertExecutor executor(&node, context.get());
    return executor.Execute();
  };

  /** @brief Insert a tuple with 4 columns' value specified */
  static bool ExecuteInsert(concurrency::TransactionContext *transaction,
                            storage::DataTable *table, const type::Value &col1,
                            const type::Value &col2, const type::Value &col3,
                            const type::Value &col4) {
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
  };

  /** @brief Insert a tuple with 3 columns' value specified */
  static bool ExecuteInsert(concurrency::TransactionContext *transaction,
                            storage::DataTable *table, const type::Value &col1,
                            const type::Value &col2, const type::Value &col3) {
    std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

    // Make tuple
    std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));

    auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
    tuple->SetValue(0, col1, testing_pool);
    tuple->SetValue(1, col2, testing_pool);
    tuple->SetValue(2, col3, testing_pool);

    auto project_info = MakeProjectInfoFromTuple(tuple.get());

    // Insert
    planner::InsertPlan node(table, std::move(project_info));
    executor::InsertExecutor executor(&node, context.get());
    return executor.Execute();
  };



  static void PopulateTable(concurrency::TransactionContext *transaction,
                            storage::DataTable *table, int num_rows) {
    // Ensure that the tile group is as expected.
    UNUSED_ATTRIBUTE const catalog::Schema *schema = table->GetSchema();
    PL_ASSERT(schema->GetColumnCount() == 4);

    for (int rowid = 0; rowid < num_rows; rowid++) {
      int populate_value = rowid;

      // First column is unique in this case
      auto col1 = type::ValueFactory::GetIntegerValue(
          PopulatedValue(populate_value, 0));

      // In case of random, make sure this column has duplicated values
      auto col2 = type::ValueFactory::GetIntegerValue(
          PopulatedValue(populate_value, 1));

      auto col3 = type::ValueFactory::GetDecimalValue(
          PopulatedValue(populate_value, 2));

      // In case of random, make sure this column has duplicated values
      auto col4 = type::ValueFactory::GetVarcharValue(
          std::to_string(PopulatedValue(populate_value, 3)));

      TestingConstraintsUtil::ExecuteInsert(transaction, table, col1, col2, col3,
                                          col4);
    }
  };

  static catalog::Column GetColumnInfo(int index,
                                       std::vector<catalog::Constraint> constraints) {
    catalog::Column column;
    switch (index) {
      // COL_A
      case 0: {
        column = catalog::Column(
            type::TypeId::INTEGER,
            type::Type::GetTypeSize(type::TypeId::INTEGER),
            "col_a",
            true);
        break;
      }
      // COL_B
      case 1: {
        column = catalog::Column(
            type::TypeId::INTEGER,
            type::Type::GetTypeSize(type::TypeId::INTEGER),
            "col_b",
            true);
        break;
      }
      // COL_C
      case 2: {
        column = catalog::Column(
            type::TypeId::DECIMAL,
            type::Type::GetTypeSize(type::TypeId::DECIMAL),
            "col_c",
            true);
        break;
      }
      // COL_D
      case 3: {
        column = catalog::Column(
            type::TypeId::VARCHAR,
            25,  // Column length.
            "col_d",
            false);
        break;
      }
      default: {
        throw ExecutorException("Invalid column index : " +
                                std::to_string(index));
      }
    }

    // Add any constraints that we have for this mofo
    for (auto col_const : constraints) {
      column.AddConstraint(col_const);
    }
    return (column);
  };

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