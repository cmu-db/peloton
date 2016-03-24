#include "concurrency/transaction_tests_util.h"
#include "backend/catalog/schema.h"
#include "backend/common/value_factory.h"
#include "backend/common/pool.h"

#include "backend/executor/executor_context.h"
#include "backend/executor/delete_executor.h"
#include "backend/executor/insert_executor.h"
#include "backend/executor/seq_scan_executor.h"
#include "backend/executor/update_executor.h"
#include "backend/executor/logical_tile_factory.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/abstract_expression.h"
#include "backend/storage/tile.h"
#include "backend/storage/tile_group.h"
#include "backend/storage/table_factory.h"
#include "backend/concurrency/transaction_manager_factory.h"

#include "executor/mock_executor.h"

#include "backend/planner/delete_plan.h"
#include "backend/planner/insert_plan.h"
#include "backend/planner/seq_scan_plan.h"
#include "backend/planner/update_plan.h"

namespace peloton {
  namespace executor {
    class ExecutorContext;
  }
  namespace planner {
    class InsertPlan;
    class ProjectInfo;
  }
  namespace test {
    storage::DataTable * TransactionTestsUtil::CreateTable() {
      auto id_column = catalog::Column(VALUE_TYPE_INTEGER,
                                       GetTypeSize(VALUE_TYPE_INTEGER), "id", true);
      auto value_column = catalog::Column(
        VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER), "value", true);

      // Create the table
      catalog::Schema *table_schema =
        new catalog::Schema({id_column, value_column});
      auto table_name = "TEST_TABLE";
      size_t tuples_per_tilegroup = 100;
      auto table = storage::TableFactory::GetDataTable(
        INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
        true, false);

      // Create index on the id column
      std::vector<oid_t> key_attrs = {0};
      auto tuple_schema = table->GetSchema();
      bool unique = true;
      auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
      key_schema->SetIndexedColumns(key_attrs);

      auto index_metadata = new index::IndexMetadata(
        "primary_btree_index", 1234, INDEX_TYPE_BTREE,
        INDEX_CONSTRAINT_TYPE_PRIMARY_KEY, tuple_schema, key_schema, unique);

      index::Index *pkey_index = index::IndexFactory::GetInstance(index_metadata);

      table->AddIndex(pkey_index);

      // Insert initial tuple
      std::unique_ptr<storage::Tuple> tuple = std::unique_ptr<storage::Tuple>(
        new storage::Tuple(table->GetSchema(), true));

      auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
      tuple->SetValue(0, ValueFactory::GetIntegerValue(0), testing_pool);
      tuple->SetValue(1, ValueFactory::GetIntegerValue(0), testing_pool);

      return table;
    }

    planner::ProjectInfo *TransactionTestsUtil::MakeProjectInfoFromTuple(const storage::Tuple *tuple) {
      planner::ProjectInfo::TargetList target_list;
      planner::ProjectInfo::DirectMapList direct_map_list;

      for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
        auto value = tuple->GetValue(col_id);
        auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
        target_list.emplace_back(col_id, expression);
      }

      return new planner::ProjectInfo(std::move(target_list),
                                      std::move(direct_map_list));
    }

    bool TransactionTestsUtil::ExecuteInsert(concurrency::Transaction *transaction, storage::DataTable *table, int id, int value)
    {
      std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(transaction));

      // Make tuple
      std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(table->GetSchema(), true));
      auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
      tuple->SetValue(0, ValueFactory::GetIntegerValue(id), testing_pool);
      tuple->SetValue(1, ValueFactory::GetIntegerValue(value), testing_pool);
      auto project_info = MakeProjectInfoFromTuple(tuple.get());

      // Insert
      planner::InsertPlan node(table, project_info);
      executor::InsertExecutor executor(&node, context.get());
      return executor.Execute();
    }

    expression::ComparisonExpression<expression::CmpEq> *TransactionTestsUtil::MakePredicate(int key)
    {
      auto tup_val_exp = new expression::TupleValueExpression(0, 0);
      auto const_val_exp = new expression::ConstantValueExpression(ValueFactory::GetIntegerValue(key));
      auto predicate = new expression::ComparisonExpression<expression::CmpEq>(EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

      return predicate;
    }

    int TransactionTestsUtil::ExecuteRead(concurrency::Transaction *transaction, storage::DataTable *table, int key)
    {
      std::unique_ptr<executor::ExecutorContext> context(
        new executor::ExecutorContext(transaction));

      // Predicate, WHERE `id`=id
      auto predicate = MakePredicate(key);

      // Seq scan
      std::vector<oid_t> column_ids = {0, 1};
      planner::SeqScanPlan seq_scan_node(table, predicate, column_ids);
      executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

      EXPECT_TRUE(seq_scan_executor.Init());
      EXPECT_TRUE(seq_scan_executor.Execute());

      std::unique_ptr<executor::LogicalTile> result_tile(seq_scan_executor.GetOutput());

      // Read nothing
      if (result_tile->GetTupleCount() == 0)
        return -1;
      else {
        EXPECT_EQ(1, result_tile->GetTupleCount());
        return result_tile->GetValue(0, 1).GetIntegerForTestsOnly();
      }
    }
    bool ExecuteDelete(__attribute__((unused)) concurrency::Transaction *transaction, __attribute__((unused)) storage::DataTable *table, __attribute__((unused)) int key)
    {

      //      // Delete
//      planner::DeletePlan delete_node(table, false);
//      executor::DeleteExecutor delete_executor(&delete_node, context.get());

      return false;
    }
    bool ExecuteUpdate(__attribute__((unused)) concurrency::Transaction *transaction, __attribute__((unused)) storage::DataTable *table, __attribute__((unused)) int key, __attribute__((unused)) int value)
    {
      return false;
    }

  }
}