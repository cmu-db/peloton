//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metrics_catalog.cpp
//
// Identification: src/catalog/index_metrics_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/index_metrics_catalog.h"

#include "expression/expression_util.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

IndexMetricsCatalog *IndexMetricsCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static IndexMetricsCatalog index_metrics_catalog{txn};
  return &index_metrics_catalog;
}

IndexMetricsCatalog::IndexMetricsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." INDEX_METRICS_CATALOG_NAME
                      " ("
                      "database_oid   INT NOT NULL, "
                      "table_oid      INT NOT NULL, "
                      "index_oid      INT NOT NULL, "
                      "reads          INT NOT NULL, "
                      "deletes        INT NOT NULL, "
                      "inserts        INT NOT NULL, "
                      "time_stamp     INT NOT NULL);",
                      txn) {
  // Add secondary index here if necessary
}

IndexMetricsCatalog::~IndexMetricsCatalog() {}

bool IndexMetricsCatalog::InsertIndexMetrics(
    oid_t database_oid, oid_t table_oid, oid_t index_oid, int64_t reads,
    int64_t deletes, int64_t inserts, int64_t time_stamp,
    type::AbstractPool *pool, concurrency::TransactionContext *txn) {

  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val0 = type::ValueFactory::GetIntegerValue(database_oid);
  auto val1 = type::ValueFactory::GetIntegerValue(table_oid);
  auto val2 = type::ValueFactory::GetIntegerValue(index_oid);
  auto val3 = type::ValueFactory::GetIntegerValue(reads);
  auto val4 = type::ValueFactory::GetIntegerValue(deletes);
  auto val5 = type::ValueFactory::GetIntegerValue(inserts);
  auto val6 = type::ValueFactory::GetIntegerValue(time_stamp);

  auto constant_expr_0 = new expression::ConstantValueExpression(
      val0);
  auto constant_expr_1 = new expression::ConstantValueExpression(
      val1);
  auto constant_expr_2 = new expression::ConstantValueExpression(
      val2);
  auto constant_expr_3 = new expression::ConstantValueExpression(
      val0);
  auto constant_expr_4 = new expression::ConstantValueExpression(
      val1);
  auto constant_expr_5 = new expression::ConstantValueExpression(
      val2);
  auto constant_expr_6 = new expression::ConstantValueExpression(
      val2);

  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];
  values.push_back(ExpressionPtr(constant_expr_0));
  values.push_back(ExpressionPtr(constant_expr_1));
  values.push_back(ExpressionPtr(constant_expr_2));
  values.push_back(ExpressionPtr(constant_expr_3));
  values.push_back(ExpressionPtr(constant_expr_4));
  values.push_back(ExpressionPtr(constant_expr_5));
  values.push_back(ExpressionPtr(constant_expr_6));

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

bool IndexMetricsCatalog::DeleteIndexMetrics(
    oid_t index_oid, concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;  // Primary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(index_oid).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

}  // namespace catalog
}  // namespace peloton
