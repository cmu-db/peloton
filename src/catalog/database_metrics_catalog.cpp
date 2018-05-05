//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// database_metrics_catalog.cpp
//
// Identification: src/catalog/query_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/database_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "expression/abstract_expression.h"
#include "expression/expression_util.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

DatabaseMetricsCatalog *DatabaseMetricsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static DatabaseMetricsCatalog database_metrics_catalog{txn};
  return &database_metrics_catalog;
}

DatabaseMetricsCatalog::DatabaseMetricsCatalog(
    concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." DATABASE_METRICS_CATALOG_NAME
                      " ("
                      "database_oid  INT NOT NULL, "
                      "txn_committed INT NOT NULL, "
                      "txn_aborted   INT NOT NULL, "
                      "time_stamp    INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

DatabaseMetricsCatalog::~DatabaseMetricsCatalog() {}

bool DatabaseMetricsCatalog::InsertDatabaseMetrics(
    oid_t database_oid, oid_t txn_committed, oid_t txn_aborted,
    oid_t time_stamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  (void) pool;
  std::vector<std::vector<ExpressionPtr>> tuples;
  tuples.emplace_back();
  auto &values = tuples[0];

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(txn_committed);
  auto val2 = type::ValueFactory::GetIntegerValue(txn_aborted);
  auto val3 = type::ValueFactory::GetIntegerValue(time_stamp);

  values.emplace_back(new expression::ConstantValueExpression(
      val0));
  values.emplace_back(new expression::ConstantValueExpression(
      val1));
  values.emplace_back(new expression::ConstantValueExpression(
      val2));
  values.emplace_back(new expression::ConstantValueExpression(
      val3));


  // Insert the tuple into catalog table
  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool DatabaseMetricsCatalog::DeleteDatabaseMetrics(
    oid_t database_oid, concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(all_column_ids);

  auto *db_oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                                    ColumnId::DATABASE_OID);
  db_oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                           catalog_table_->GetOid(),
                           ColumnId::DATABASE_OID);
  expression::AbstractExpression *db_oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(database_oid).Copy());
  expression::AbstractExpression *predicate =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, db_oid_expr, db_oid_const_expr);
  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

}  // namespace catalog
}  // namespace peloton
