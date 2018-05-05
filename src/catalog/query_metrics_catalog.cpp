//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/query_metrics_catalog.h"

#include "codegen/buffering_consumer.h"
#include "expression/expression_util.h"
#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

QueryMetricsCatalog::QueryMetricsCatalog(const std::string &database_name,
                                         concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." QUERY_METRICS_CATALOG_NAME
                          " ("
                          "query_name   VARCHAR NOT NULL PRIMARY KEY, "
                          "database_oid INT NOT NULL PRIMARY KEY, "
                          "num_params   INT NOT NULL, "
                          "param_types    VARBINARY, "
                          "param_formats  VARBINARY, "
                          "param_values   VARBINARY, "
                          "reads    INT NOT NULL, "
                          "updates  INT NOT NULL, "
                          "deletes  INT NOT NULL, "
                          "inserts  INT NOT NULL, "
                          "latency  INT NOT NULL, "
                          "cpu_time INT NOT NULL, "
                          "time_stamp INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

QueryMetricsCatalog::~QueryMetricsCatalog() {}

bool QueryMetricsCatalog::InsertQueryMetrics(
    const std::string &name, oid_t database_oid, int64_t num_params,
    const stats::QueryMetric::QueryParamBuf &type_buf,
    const stats::QueryMetric::QueryParamBuf &format_buf,
    const stats::QueryMetric::QueryParamBuf &value_buf, int64_t reads,
    int64_t updates, int64_t deletes, int64_t inserts, int64_t latency,
    int64_t cpu_time, int64_t time_stamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {

  std::vector<std::vector<ExpressionPtr>> tuples;
  tuples.emplace_back();
  auto &values = tuples[0];

  auto val0 = type::ValueFactory::GetVarcharValue(name, pool);
  auto val1 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(num_params);

  auto val3 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);
  auto val4 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);
  auto val5 = type::ValueFactory::GetNullValueByType(type::TypeId::VARBINARY);

  if (num_params != 0) {
    val3 =
        type::ValueFactory::GetVarbinaryValue(type_buf.buf, type_buf.len, true);
    val4 = type::ValueFactory::GetVarbinaryValue(format_buf.buf, format_buf.len,
                                                 true);
    val5 = type::ValueFactory::GetVarbinaryValue(value_buf.buf, value_buf.len,
                                                 true);
  }

  auto val6 = type::ValueFactory::GetIntegerValue(reads);
  auto val7 = type::ValueFactory::GetIntegerValue(updates);
  auto val8 = type::ValueFactory::GetIntegerValue(deletes);
  auto val9 = type::ValueFactory::GetIntegerValue(inserts);
  auto val10 = type::ValueFactory::GetIntegerValue(latency);
  auto val11 = type::ValueFactory::GetIntegerValue(cpu_time);
  auto val12 = type::ValueFactory::GetIntegerValue(time_stamp);

  values.emplace_back(new expression::ConstantValueExpression(
      val0));
  values.emplace_back(new expression::ConstantValueExpression(
      val1));
  values.emplace_back(new expression::ConstantValueExpression(
      val2));
  values.emplace_back(new expression::ConstantValueExpression(
      val3));
  values.emplace_back(new expression::ConstantValueExpression(
      val4));
  values.emplace_back(new expression::ConstantValueExpression(
      val5));
  values.emplace_back(new expression::ConstantValueExpression(
      val6));
  values.emplace_back(new expression::ConstantValueExpression(
      val7));
  values.emplace_back(new expression::ConstantValueExpression(
      val8));
  values.emplace_back(new expression::ConstantValueExpression(
      val9));
  values.emplace_back(new expression::ConstantValueExpression(
      val10));
  values.emplace_back(new expression::ConstantValueExpression(
      val11));
  values.emplace_back(new expression::ConstantValueExpression(
      val12));

  // Insert the tuple
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool QueryMetricsCatalog::DeleteQueryMetrics(
    const std::string &name, concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(type::TypeId::VARCHAR, 0,
                                           ColumnId::NAME);
  name_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                               catalog_table_->GetOid(),
                               ColumnId::NAME);
  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr,
          name_const_expr);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_OID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_OID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, name_equality_expr,
          db_oid_equality_expr);
  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

stats::QueryMetric::QueryParamBuf QueryMetricsCatalog::GetParamTypes(
    const std::string &name, concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(
          type::TypeId::VARCHAR, 0,
          ColumnId::NAME);

  name_expr->SetBoundOid(
      catalog_table_->GetDatabaseOid(),
      catalog_table_->GetOid(),
      ColumnId::NAME);

  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_OID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_OID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, name_equality_expr,
          db_oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  stats::QueryMetric::QueryParamBuf param_types;
  PELOTON_ASSERT(result_tuples.size() <= 1);  // unique
  if (!result_tuples.empty()) {
    auto param_types_value = result_tuples[0].GetValue(ColumnId::PARAM_TYPES);
    param_types.buf = const_cast<uchar *>(
        reinterpret_cast<const uchar *>(param_types_value.GetData()));
    param_types.len = param_types_value.GetLength();
  }

  return param_types;
}

int64_t QueryMetricsCatalog::GetNumParams(
    const std::string &name, concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *name_expr =
      new expression::TupleValueExpression(
          type::TypeId::VARCHAR, 0,
          ColumnId::NAME);

  name_expr->SetBoundOid(
      catalog_table_->GetDatabaseOid(),
      catalog_table_->GetOid(),
      ColumnId::NAME);

  expression::AbstractExpression *name_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetVarcharValue(name, nullptr).Copy());
  expression::AbstractExpression *name_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, name_expr, name_const_expr);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::DATABASE_OID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                            catalog_table_->GetOid(),
                            ColumnId::DATABASE_OID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *db_oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);

  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ConjunctionFactory(
          ExpressionType::CONJUNCTION_AND, name_equality_expr,
          db_oid_equality_expr);

  std::vector<codegen::WrappedTuple> result_tuples =
      GetResultWithCompiledSeqScan(column_ids, predicate, txn);

  int64_t num_params = 0;
  PELOTON_ASSERT(result_tuples.size() <= 1);  // unique
  if (!result_tuples.empty()) {
    num_params = result_tuples[0].GetValue(ColumnId::NUM_PARAMS)
                     .GetAs<int>();  // After projection left 1 column
  }

  return num_params;
}

}  // namespace catalog
}  // namespace peloton
