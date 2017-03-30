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

  LOG_TRACE("Removing tuple with id %d from table %s", (int)id,
            table->GetName().c_str());
  LOG_TRACE("Transaction ID: %d", (int)txn->GetTransactionId());
  // Delete
  planner::DeletePlan delete_node(table, false);
  executor::DeleteExecutor delete_executor(&delete_node, context.get());

  // Predicate
  // WHERE id_in_table = id
  expression::TupleValueExpression *tup_val_exp =
      new expression::TupleValueExpression(type::Type::INTEGER, 0, 0);
  auto tmp_value = type::ValueFactory::GetIntegerValue(id);
  expression::ConstantValueExpression *const_val_exp =
      new expression::ConstantValueExpression(tmp_value);
  auto predicate = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, tup_val_exp, const_val_exp);

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
    const catalog::Schema *schema, oid_t database_id, std::string database_name,
    type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
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
    const catalog::Schema *schema, oid_t database_id, int64_t commit,
    int64_t abort, int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(commit);
  auto val3 = type::ValueFactory::GetIntegerValue(abort);
  auto val4 = type::ValueFactory::GetIntegerValue(time_stamp);

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
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    int64_t reads, int64_t updates, int64_t deletes, int64_t inserts,
    int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(table_id);
  auto val3 = type::ValueFactory::GetIntegerValue(reads);
  auto val4 = type::ValueFactory::GetIntegerValue(updates);
  auto val5 = type::ValueFactory::GetIntegerValue(deletes);
  auto val6 = type::ValueFactory::GetIntegerValue(inserts);
  auto val7 = type::ValueFactory::GetIntegerValue(time_stamp);

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
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    oid_t index_id, int64_t reads, int64_t deletes, int64_t inserts,
    int64_t time_stamp) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(table_id);
  auto val3 = type::ValueFactory::GetIntegerValue(index_id);
  auto val4 = type::ValueFactory::GetIntegerValue(reads);
  auto val5 = type::ValueFactory::GetIntegerValue(deletes);
  auto val6 = type::ValueFactory::GetIntegerValue(inserts);
  auto val7 = type::ValueFactory::GetIntegerValue(time_stamp);

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
    const catalog::Schema *schema, std::string query_name, oid_t database_id,
    int64_t num_params, stats::QueryMetric::QueryParamBuf type_buf,
    stats::QueryMetric::QueryParamBuf format_buf,
    stats::QueryMetric::QueryParamBuf val_buf, int64_t reads, int64_t updates,
    int64_t deletes, int64_t inserts, int64_t latency, int64_t cpu_time,
    int64_t time_stamp, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));

  auto val1 = type::ValueFactory::GetVarcharValue(query_name, nullptr);
  auto val2 = type::ValueFactory::GetIntegerValue(database_id);
  auto val3 = type::ValueFactory::GetIntegerValue(num_params);

  type::Value param_type =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  type::Value param_format =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);
  type::Value param_value =
      type::ValueFactory::GetNullValueByType(type::Type::VARBINARY);

  if (num_params != 0) {
    param_type =
        type::ValueFactory::GetVarbinaryValue(type_buf.buf, type_buf.len, false);
    param_format =
        type::ValueFactory::GetVarbinaryValue(format_buf.buf, format_buf.len, false);
    param_value =
        type::ValueFactory::GetVarbinaryValue(val_buf.buf, val_buf.len, false);
  }

  auto val7 = type::ValueFactory::GetIntegerValue(reads);
  auto val8 = type::ValueFactory::GetIntegerValue(updates);
  auto val9 = type::ValueFactory::GetIntegerValue(deletes);
  auto val10 = type::ValueFactory::GetIntegerValue(inserts);
  auto val11 = type::ValueFactory::GetIntegerValue(latency);
  auto val12 = type::ValueFactory::GetIntegerValue(cpu_time);
  auto val13 = type::ValueFactory::GetIntegerValue(time_stamp);

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);

  tuple->SetValue(3, param_type, pool);
  tuple->SetValue(4, param_format, pool);
  tuple->SetValue(5, param_value, pool);

  tuple->SetValue(6, val7, nullptr);
  tuple->SetValue(7, val8, nullptr);
  tuple->SetValue(8, val9, nullptr);
  tuple->SetValue(9, val10, nullptr);
  tuple->SetValue(10, val11, nullptr);
  tuple->SetValue(11, val12, nullptr);
  tuple->SetValue(12, val13, nullptr);

  return std::move(tuple);
}

/**
 * Generate a table catalog tuple
 * Input: The table schema, the table id, the table name, the database id, and
 * the database name
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetTableCatalogTuple(
    const catalog::Schema *schema, oid_t table_id, std::string table_name,
    oid_t database_id, std::string database_name, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(table_id);
  auto val2 = type::ValueFactory::GetVarcharValue(table_name, nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(database_id);
  auto val4 = type::ValueFactory::GetVarcharValue(database_name, nullptr);
  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  return std::move(tuple);
}

/**
 * Generate an index catalog tuple
 * Input: The table schema, the index_catalog_object
 * Returns: The generated tuple
 */
std::unique_ptr<storage::Tuple> GetIndexCatalogTuple(
    const catalog::Schema *schema, const catalog::IndexCatalogObject *index_catalog_object, type::AbstractPool *pool) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(index_catalog_object->GetOid());
  auto val2 = type::ValueFactory::GetVarcharValue(index_catalog_object->GetName(), nullptr);
  auto val3 = type::ValueFactory::GetIntegerValue(index_catalog_object->table_oid);
  auto val4 = type::ValueFactory::GetIntegerValue(index_catalog_object->database_oid);
  auto val5 = type::ValueFactory::GetBooleanValue(index_catalog_object->HasUniqueKeys());

  tuple->SetValue(0, val1, pool);
  tuple->SetValue(1, val2, pool);
  tuple->SetValue(2, val3, pool);
  tuple->SetValue(3, val4, pool);
  tuple->SetValue(4, val5, pool);
  return std::move(tuple);
}

}
}
