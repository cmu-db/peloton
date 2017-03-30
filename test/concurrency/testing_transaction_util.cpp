//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_tests_util.cpp
//
// Identification: test/concurrency/transaction_tests_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/testing_transaction_util.h"

#include "catalog/catalog.h"
#include "executor/delete_executor.h"
#include "executor/executor_context.h"
#include "executor/index_scan_executor.h"
#include "executor/insert_executor.h"
#include "executor/logical_tile_factory.h"
#include "executor/mock_executor.h"
#include "executor/seq_scan_executor.h"
#include "executor/update_executor.h"
#include "expression/expression_util.h"
#include "planner/delete_plan.h"
#include "planner/index_scan_plan.h"
#include "planner/insert_plan.h"
#include "storage/database.h"
#include "storage/tile.h"

namespace peloton {
namespace executor {
class ExecutorContext;
}
namespace planner {
class InsertPlan;
class ProjectInfo;
}
namespace test {

storage::DataTable *TestingTransactionUtil::CreateCombinedPrimaryKeyTable() {
  auto id_column =
      catalog::Column(type::Type::INTEGER,
                      type::Type::GetTypeSize(type::Type::INTEGER), "id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, "not_null"));
  auto value_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "value", true);
  value_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, "not_null"));

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});
  auto table_name = "TEST_TABLE";
  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the (id, value) column
  std::vector<oid_t> key_attrs = {0, 1};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_catalog_object = new catalog::IndexCatalogObject(
      "primary_btree_index", 1234, INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_catalog_object));

  table->AddIndex(pkey_index);

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < 10; i++) {
    ExecuteInsert(txn, table, i, i);
  }
  txn_manager.CommitTransaction(txn);

  return table;
}

storage::DataTable *TestingTransactionUtil::CreatePrimaryKeyUniqueKeyTable() {
  auto id_column =
      catalog::Column(type::Type::INTEGER,
                      type::Type::GetTypeSize(type::Type::INTEGER), "id", true);
  id_column.AddConstraint(
      catalog::Constraint(ConstraintType::NOTNULL, "not_null"));
  auto value_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "value", true);

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});
  auto table_name = "TEST_TABLE";
  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      INVALID_OID, INVALID_OID, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create primary index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_catalog_object = new catalog::IndexCatalogObject(
      "primary_btree_index", 1234, INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::PRIMARY_KEY, tuple_schema, key_schema, key_attrs,
      unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_catalog_object));

  table->AddIndex(pkey_index);

  // Create unique index on the value column
  std::vector<oid_t> key_attrs2 = {1};
  auto tuple_schema2 = table->GetSchema();
  bool unique2 = false;
  auto key_schema2 = catalog::Schema::CopySchema(tuple_schema2, key_attrs2);
  key_schema2->SetIndexedColumns(key_attrs2);
  auto index_catalog_object2 = new catalog::IndexCatalogObject(
      "unique_btree_index", 1235, INVALID_OID, INVALID_OID, IndexType::BWTREE,
      IndexConstraintType::UNIQUE, tuple_schema2, key_schema2, key_attrs2,
      unique2);

  std::shared_ptr<index::Index> ukey_index(
      index::IndexFactory::GetIndex(index_catalog_object2));

  table->AddIndex(ukey_index);

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < 10; i++) {
    ExecuteInsert(txn, table, i, i);
  }
  txn_manager.CommitTransaction(txn);

  return table;
}

storage::DataTable *TestingTransactionUtil::CreateTable(
    int num_key, std::string table_name, oid_t database_id, oid_t relation_id,
    oid_t index_oid, bool need_primary_index) {
  auto id_column =
      catalog::Column(type::Type::INTEGER,
                      type::Type::GetTypeSize(type::Type::INTEGER), "id", true);
  auto value_column = catalog::Column(
      type::Type::INTEGER, type::Type::GetTypeSize(type::Type::INTEGER),
      "value", true);

  // Create the table
  catalog::Schema *table_schema =
      new catalog::Schema({id_column, value_column});

  size_t tuples_per_tilegroup = 100;
  auto table = storage::TableFactory::GetDataTable(
      database_id, relation_id, table_schema, table_name, tuples_per_tilegroup,
      true, false);

  // Create index on the id column
  std::vector<oid_t> key_attrs = {0};
  auto tuple_schema = table->GetSchema();
  bool unique = false;
  auto key_schema = catalog::Schema::CopySchema(tuple_schema, key_attrs);
  key_schema->SetIndexedColumns(key_attrs);

  auto index_catalog_object = new catalog::IndexCatalogObject(
      "primary_btree_index", index_oid, INVALID_OID, INVALID_OID,
      IndexType::BWTREE, need_primary_index ? IndexConstraintType::PRIMARY_KEY
                                            : IndexConstraintType::DEFAULT,
      tuple_schema, key_schema, key_attrs, unique);

  std::shared_ptr<index::Index> pkey_index(
      index::IndexFactory::GetIndex(index_catalog_object));

  table->AddIndex(pkey_index);

  // add this table to current database
  auto catalog = catalog::Catalog::GetInstance();
  try {
    storage::Database *db = catalog->GetDatabaseWithOid(database_id);
    db->AddTable(table);
  } catch (CatalogException &e) {
  }

  // Insert tuple
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  for (int i = 0; i < num_key; i++) {
    ExecuteInsert(txn, table, i, 0);
  }
  txn_manager.CommitTransaction(txn);

  return table;
}

std::unique_ptr<const planner::ProjectInfo>
TestingTransactionUtil::MakeProjectInfoFromTuple(const storage::Tuple *tuple) {
  TargetList target_list;
  DirectMapList direct_map_list;

  for (oid_t col_id = START_OID; col_id < tuple->GetColumnCount(); col_id++) {
    type::Value value = (tuple->GetValue(col_id));
    auto expression = expression::ExpressionUtil::ConstantValueFactory(value);
    target_list.emplace_back(col_id, expression);
  }

  return std::unique_ptr<const planner::ProjectInfo>(new planner::ProjectInfo(
      std::move(target_list), std::move(direct_map_list)));
}

bool TestingTransactionUtil::ExecuteInsert(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id,
                                         int value) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Make tuple
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(table->GetSchema(), true));
  auto testing_pool = TestingHarness::GetInstance().GetTestingPool();
  tuple->SetValue(0, type::ValueFactory::GetIntegerValue(id), testing_pool);
  tuple->SetValue(1, type::ValueFactory::GetIntegerValue(value), testing_pool);
  std::unique_ptr<const planner::ProjectInfo> project_info{
      MakeProjectInfoFromTuple(tuple.get())};

  // Insert
  planner::InsertPlan node(table, std::move(project_info));
  executor::InsertExecutor executor(&node, context.get());
  return executor.Execute();
}

expression::ComparisonExpression *TestingTransactionUtil::MakePredicate(int id) {
  auto tup_val_exp =
      new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);
  auto const_val_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(id));
  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tup_val_exp, const_val_exp);

  return predicate;
}

planner::IndexScanPlan::IndexScanDesc MakeIndexDesc(storage::DataTable *table,
                                                    int id) {
  auto index = table->GetIndex(0);
  std::vector<expression::AbstractExpression *> runtime_keys;
  std::vector<ExpressionType> expr_types;
  std::vector<type::Value> values;

  std::vector<oid_t> key_column_ids = {0};

  expr_types.push_back(ExpressionType::COMPARE_EQUAL);

  values.push_back(type::ValueFactory::GetIntegerValue(id).Copy());

  return planner::IndexScanPlan::IndexScanDesc(
      index, key_column_ids, expr_types, values, runtime_keys);
}

bool TestingTransactionUtil::ExecuteRead(concurrency::Transaction *transaction,
                                       storage::DataTable *table, int id,
                                       int &result, bool select_for_update) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // index scan
  std::vector<oid_t> column_ids = {0, 1};
  planner::IndexScanPlan idx_scan_node(
      table, nullptr, column_ids, MakeIndexDesc(table, id), select_for_update);
  executor::IndexScanExecutor idx_scan_executor(&idx_scan_node, context.get());

  EXPECT_TRUE(idx_scan_executor.Init());
  if (idx_scan_executor.Execute() == false) {
    result = -1;
    return false;
  }

  std::unique_ptr<executor::LogicalTile> result_tile(
      idx_scan_executor.GetOutput());

  // Read nothing
  if (result_tile->GetTupleCount() == 0)
    result = -1;
  else {
    EXPECT_EQ(1, result_tile->GetTupleCount());
    type::Value val = (result_tile->GetValue(0, 1));
    result = val.GetAs<int32_t>();
  }

  return true;
}
bool TestingTransactionUtil::ExecuteDelete(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id,
                                         bool select_for_update) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  auto predicate = MakePredicate(id);

  // Scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(new planner::SeqScanPlan(
      table, predicate, column_ids, select_for_update));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(delete_executor.Init());

  return delete_executor.Execute();
}
bool TestingTransactionUtil::ExecuteUpdate(concurrency::Transaction *transaction,
                                         storage::DataTable *table, int id,
                                         int value, bool select_for_update) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  auto update_val = type::ValueFactory::GetIntegerValue(value);

  // ProjectInfo
  TargetList target_list;
  DirectMapList direct_map_list;
  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));
  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));

  // Update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Index scan
  std::vector<oid_t> column_ids = {0};
  std::unique_ptr<planner::IndexScanPlan> idx_scan_node(
      new planner::IndexScanPlan(table, nullptr, column_ids,
                                 MakeIndexDesc(table, id), select_for_update));
  executor::IndexScanExecutor idx_scan_executor(idx_scan_node.get(),
                                                context.get());

  update_node.AddChild(std::move(idx_scan_node));
  update_executor.AddChild(&idx_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  return update_executor.Execute();
}

bool TestingTransactionUtil::ExecuteUpdateByValue(concurrency::Transaction *txn,
                                                storage::DataTable *table,
                                                int old_value, int new_value,
                                                bool select_for_update) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  auto update_val = type::ValueFactory::GetIntegerValue(new_value);

  // ProjectInfo
  TargetList target_list;
  DirectMapList direct_map_list;
  target_list.emplace_back(
      1, expression::ExpressionUtil::ConstantValueFactory(update_val));
  direct_map_list.emplace_back(0, std::pair<oid_t, oid_t>(0, 0));

  // Update plan
  std::unique_ptr<const planner::ProjectInfo> project_info(
      new planner::ProjectInfo(std::move(target_list),
                               std::move(direct_map_list)));
  planner::UpdatePlan update_node(table, std::move(project_info));

  executor::UpdateExecutor update_executor(&update_node, context.get());

  // Predicate
  auto tup_val_exp =
      new expression::TupleValueExpression(type::Type::INTEGER, 0, 1);
  auto const_val_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(old_value));
  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0, 1};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(new planner::SeqScanPlan(
      table, predicate, column_ids, select_for_update));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  update_node.AddChild(std::move(seq_scan_node));
  update_executor.AddChild(&seq_scan_executor);

  EXPECT_TRUE(update_executor.Init());
  return update_executor.Execute();
}

bool TestingTransactionUtil::ExecuteScan(concurrency::Transaction *transaction,
                                       std::vector<int> &results,
                                       storage::DataTable *table, int id,
                                       bool select_for_update) {
  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(transaction));

  // Predicate, WHERE `id`>=id1
  auto tup_val_exp =
      new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);
  auto const_val_exp = new expression::ConstantValueExpression(
      type::ValueFactory::GetIntegerValue(id));

  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0, 1};
  planner::SeqScanPlan seq_scan_node(table, predicate, column_ids,
                                     select_for_update);
  executor::SeqScanExecutor seq_scan_executor(&seq_scan_node, context.get());

  EXPECT_TRUE(seq_scan_executor.Init());
  if (seq_scan_executor.Execute() == false) return false;

  std::unique_ptr<executor::LogicalTile> result_tile(
      seq_scan_executor.GetOutput());

  for (size_t i = 0; i < result_tile->GetTupleCount(); i++) {
    type::Value val = (result_tile->GetValue(i, 1));
    results.push_back(val.GetAs<int32_t>());
  }
  return true;
}
}
}
