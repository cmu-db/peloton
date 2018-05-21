//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metrics_catalog.cpp
//
// Identification: src/catalog/table_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/table_metrics_catalog.h"

#include "executor/logical_tile.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TableMetricsCatalog::TableMetricsCatalog(const std::string &database_name,
                                         concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." TABLE_METRICS_CATALOG_NAME
                          " ("
                          "table_oid      INT NOT NULL, "
                          "reads          INT NOT NULL, "
                          "updates        INT NOT NULL, "
                          "deletes        INT NOT NULL, "
                          "inserts        INT NOT NULL, "
                          "time_stamp     INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

TableMetricsCatalog::~TableMetricsCatalog() {}

bool TableMetricsCatalog::InsertTableMetrics(
    oid_t table_oid, int64_t reads, int64_t updates, int64_t deletes,
    int64_t inserts, int64_t time_stamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {

  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val1 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(reads);
  auto val3 = type::ValueFactory::GetIntegerValue(updates);
  auto val4 = type::ValueFactory::GetIntegerValue(deletes);
  auto val5 = type::ValueFactory::GetIntegerValue(inserts);
  auto val6 = type::ValueFactory::GetIntegerValue(time_stamp);


  auto constant_expr_1 = new expression::ConstantValueExpression(
      val1);
  auto constant_expr_2 = new expression::ConstantValueExpression(
      val2);
  auto constant_expr_3 = new expression::ConstantValueExpression(
      val3);
  auto constant_expr_4 = new expression::ConstantValueExpression(
      val4);
  auto constant_expr_5 = new expression::ConstantValueExpression(
      val5);
  auto constant_expr_6 = new expression::ConstantValueExpression(
      val6);

  tuples.emplace_back();
  auto &values = tuples[0];
  values.emplace_back(constant_expr_1);
  values.emplace_back(constant_expr_2);
  values.emplace_back(constant_expr_3);
  values.emplace_back(constant_expr_4);
  values.emplace_back(constant_expr_5);
  values.emplace_back(constant_expr_6);

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool TableMetricsCatalog::DeleteTableMetrics(
    oid_t table_oid, concurrency::TransactionContext *txn) {

  std::vector<oid_t> column_ids(all_column_ids);

  auto *oid_expr =
      new expression::TupleValueExpression(type::TypeId::INTEGER, 0,
                                           ColumnId::TABLE_OID);

  oid_expr->SetBoundOid(catalog_table_->GetDatabaseOid(),
                        catalog_table_->GetOid(),
                        ColumnId::TABLE_OID);

  expression::AbstractExpression *oid_const_expr =
      expression::ExpressionUtil::ConstantValueFactory(
          type::ValueFactory::GetIntegerValue(table_oid).Copy());
  expression::AbstractExpression *oid_equality_expr =
      expression::ExpressionUtil::ComparisonFactory(
          ExpressionType::COMPARE_EQUAL, oid_expr, oid_const_expr);

  expression::AbstractExpression *predicate = oid_equality_expr;

  return DeleteWithCompiledSeqScan(column_ids, predicate, txn);
}

}  // namespace catalog
}  // namespace peloton
