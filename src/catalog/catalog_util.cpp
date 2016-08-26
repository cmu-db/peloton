//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// catalog_util.cpp
//
// Identification: src/catalog/catalog_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog_util.h"

namespace peloton {

namespace catalog {
/**
 * Inserts a tuple in a table
 */
void InsertTuple(storage::DataTable *table,
                 std::unique_ptr<storage::Tuple> tuple,
                 concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));
  planner::InsertPlan node(table, std::move(tuple));
  executor::InsertExecutor executor(&node, context.get());
  executor.Init();
  executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

void DeleteTuple(storage::DataTable *table, oid_t id,
                 concurrency::Transaction *txn) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool single_statement_txn = false;

  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }

  std::unique_ptr<executor::ExecutorContext> context(
      new executor::ExecutorContext(txn));

  LOG_INFO("Removing tuple with id %d from table %s", (int)id,
           table->GetName().c_str());
  LOG_INFO("Transaction ID: %d", (int)txn->GetTransactionId());
  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Predicate
  // WHERE id_in_table = id
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(
          ValueFactory::GetIntegerValue(id));
  auto predicate = new expression::ComparisonExpression<expression::CmpEq>(
      EXPRESSION_TYPE_COMPARE_EQUAL, tup_val_exp, const_val_exp);

  // Seq scan
  std::vector<oid_t> column_ids = {0, 1};
  std::unique_ptr<planner::SeqScanPlan> seq_scan_node(
      new planner::SeqScanPlan(table, predicate, column_ids));
  executor::SeqScanExecutor seq_scan_executor(seq_scan_node.get(),
                                              context.get());

  // Parent-Child relationship
  delete_node.AddChild(std::move(seq_scan_node));
  delete_executor.AddChild(&seq_scan_executor);
  delete_executor.Init();
  delete_executor.Execute();

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

/**
 * Generate a database catalog tuple
 * Input: The table schema, the database id, the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(
    catalog::Schema *schema, oid_t database_id, std::string database_name) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  Value val1 = ValueFactory::GetIntegerValue(database_id);
  Value val2 = ValueFactory::GetStringValue(database_name, nullptr);
  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  return std::move(tuple);
}

/**
 * Generate a table catalog tuple
 * Input: The table schema, the table id, the table name, the database id, and
 * the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableCatalogTuple(
    catalog::Schema *schema, oid_t table_id, std::string table_name,
    oid_t database_id, std::string database_name) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  Value val1 = ValueFactory::GetIntegerValue(table_id);
  Value val2 = ValueFactory::GetStringValue(table_name, nullptr);
  Value val3 = ValueFactory::GetIntegerValue(database_id);
  Value val4 = ValueFactory::GetStringValue(database_name, nullptr);
  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  return std::move(tuple);
}
}
}
