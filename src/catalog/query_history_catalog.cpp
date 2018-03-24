//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_history_catalog.cpp
//
// Identification: src/catalog/query_history_catalog.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/query_history_catalog.h"

#include "catalog/catalog.h"
#include "expression/expression_util.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

QueryHistoryCatalog &QueryHistoryCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static QueryHistoryCatalog query_history_catalog{txn};
  return query_history_catalog;
}

QueryHistoryCatalog::QueryHistoryCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." QUERY_HISTORY_CATALOG_NAME
                      " ("
                      "query_string   VARCHAR NOT NULL, "
                      "fingerprint    VARCHAR NOT NULL, "
                      "timestamp      TIMESTAMP NOT NULL);",
                      txn) {}

QueryHistoryCatalog::~QueryHistoryCatalog() = default;

bool QueryHistoryCatalog::InsertQueryHistory(
    const std::string &query_string, const std::string &fingerprint,
    uint64_t timestamp, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {

  (void) pool;
  // Create the tuple first
  std::vector<std::vector<ExpressionPtr>> tuples;

  auto val0 = type::ValueFactory::GetVarcharValue(query_string);
  auto val1 = type::ValueFactory::GetVarcharValue(fingerprint);
  auto val2 = type::ValueFactory::GetTimestampValue(timestamp);

  auto constant_expr_0 = new expression::ConstantValueExpression(
      val0);
  auto constant_expr_1 = new expression::ConstantValueExpression(
      val1);
  auto constant_expr_2 = new expression::ConstantValueExpression(
      val2);

  tuples.push_back(std::vector<ExpressionPtr>());
  auto &values = tuples[0];
  values.push_back(ExpressionPtr(constant_expr_0));
  values.push_back(ExpressionPtr(constant_expr_1));
  values.push_back(ExpressionPtr(constant_expr_2));

  return InsertTupleWithCompiledPlan(&tuples, txn);
}

}  // namespace catalog
}  // namespace peloton
