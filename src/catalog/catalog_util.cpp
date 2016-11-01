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
      new expression::TupleValueExpression(common::Type::INTEGER, 0, 0);
  auto tmp_value = common::ValueFactory::GetIntegerValue(id);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(tmp_value);
  auto predicate = new expression::ComparisonExpression(
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
std::unique_ptr<storage::Tuple> GetDatabaseCatalogTuple(catalog::Schema *schema,
		oid_t database_id,
		std::string database_name, common::VarlenPool *pool){
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = common::ValueFactory::GetIntegerValue(database_id);
  auto val2 = common::ValueFactory::GetVarcharValue(database_name, nullptr);
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  return std::move(tuple);
}

/**
 * Generate a database metric tuple
 * Input: The table schema, the database id, number of txn committed,
 * number of txn aborted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetDatabaseMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, int64_t commit, int64_t abort,
    int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = common::ValueFactory::GetIntegerValue(database_id);
  auto val2 = common::ValueFactory::GetIntegerValue(commit);
  auto val3 = common::ValueFactory::GetIntegerValue(abort);
  auto val4 = common::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  return std::move(tuple);
}

/**
 * Generate a table metric tuple
 * Input: The table schema, the database id, the table id, number of tuples
 * read, updated, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, oid_t table_id, int64_t reads,
    int64_t updates, int64_t deletes, int64_t inserts, int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = common::ValueFactory::GetIntegerValue(database_id);
  auto val2 = common::ValueFactory::GetIntegerValue(table_id);
  auto val3 = common::ValueFactory::GetIntegerValue(reads);
  auto val4 = common::ValueFactory::GetIntegerValue(updates);
  auto val5 = common::ValueFactory::GetIntegerValue(deletes);
  auto val6 = common::ValueFactory::GetIntegerValue(inserts);
  auto val7 = common::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  tuple->SetValue(4, val5, nullptr);
  tuple->SetValue(5, val6, nullptr);
  tuple->SetValue(6, val7, nullptr);
  return std::move(tuple);
}

/**
 * Generate a index metric tuple
 * Input: The table schema, the database id, the table id, the index id,
 * number of tuples read, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetIndexMetricsCatalogTuple(
    catalog::Schema *schema, oid_t database_id, oid_t table_id, oid_t index_id,
    int64_t reads, int64_t deletes, int64_t inserts, int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = common::ValueFactory::GetIntegerValue(database_id);
  auto val2 = common::ValueFactory::GetIntegerValue(table_id);
  auto val3 = common::ValueFactory::GetIntegerValue(index_id);
  auto val4 = common::ValueFactory::GetIntegerValue(reads);
  auto val5 = common::ValueFactory::GetIntegerValue(deletes);
  auto val6 = common::ValueFactory::GetIntegerValue(inserts);
  auto val7 = common::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  tuple->SetValue(4, val5, nullptr);
  tuple->SetValue(5, val6, nullptr);
  tuple->SetValue(6, val7, nullptr);
  return std::move(tuple);
}

/**
 * Generate a query metric tuple
 * Input: The table schema, the query string, database id, number of
 * tuples read, updated, deleted inserted, the timestamp
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetQueryMetricsCatalogTuple(
    catalog::Schema *schema, std::string query_name, oid_t database_id,
    int64_t num_params, std::vector<uchar> &format_buf,
    std::shared_ptr<std::vector<uchar>> val_buf, int64_t reads, int64_t updates,
    int64_t deletes, int64_t inserts, int64_t latency, int64_t cpu_time,
    int64_t time_stamp, common::VarlenPool *pool) {

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  auto val1 = common::ValueFactory::GetVarcharValue(query_name, nullptr);
  auto val2 = common::ValueFactory::GetIntegerValue(database_id);
  auto val3 = common::ValueFactory::GetIntegerValue(num_params);

  common::Value param_format =
      common::ValueFactory::GetNullValueByType(common::Type::VARBINARY).Copy();
  common::Value param_value =
      common::ValueFactory::GetNullValueByType(common::Type::VARBINARY).Copy();

  if (num_params != 0) {
    param_format = common::ValueFactory::GetVarbinaryValue(
        val_buf->data(), val_buf->size()).Copy();
    param_value = common::ValueFactory::GetVarbinaryValue(
        format_buf.data(), format_buf.size()).Copy();
  }

  auto val6 = common::ValueFactory::GetIntegerValue(reads);
  auto val7 = common::ValueFactory::GetIntegerValue(updates);
  auto val8 = common::ValueFactory::GetIntegerValue(deletes);
  auto val9 = common::ValueFactory::GetIntegerValue(inserts);
  auto val10 = common::ValueFactory::GetIntegerValue(latency);
  auto val11 = common::ValueFactory::GetIntegerValue(cpu_time);
  auto val12 = common::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);

  tuple->SetValue(3, param_format, pool);
  tuple->SetValue(4, param_value, pool);

  tuple->SetValue(5, val6, nullptr);
  tuple->SetValue(6, val7, nullptr);
  tuple->SetValue(7, val8, nullptr);
  tuple->SetValue(8, val9, nullptr);
  tuple->SetValue(9, val10, nullptr);
  tuple->SetValue(10, val11, nullptr);
  tuple->SetValue(11, val12, nullptr);
  return std::move(tuple);
}

/**
 * Generate a table catalog tuple
 * Input: The table schema, the table id, the table name, the database id, and
 * the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableCatalogTuple(catalog::Schema *schema,
		oid_t table_id, std::string table_name, oid_t database_id,
    std::string database_name, common::VarlenPool *pool){
	std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
	auto val1 = common::ValueFactory::GetIntegerValue(table_id);
	auto val2 = common::ValueFactory::GetVarcharValue(table_name, nullptr);
	auto val3 = common::ValueFactory::GetIntegerValue(database_id);
	auto val4 = common::ValueFactory::GetVarcharValue(database_name, nullptr);
	tuple->SetValue(0, val1, pool);
	tuple->SetValue(1, val2, pool);
	tuple->SetValue(2, val3, pool);
	tuple->SetValue(3, val4, pool);
	return std::move(tuple);
}
}
}
