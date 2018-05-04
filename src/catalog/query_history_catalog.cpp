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
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(query_string);
  auto val1 = type::ValueFactory::GetVarcharValue(fingerprint);
  auto val2 = type::ValueFactory::GetTimestampValue(timestamp);

  tuple->SetValue(ColumnId::QUERY_STRING, val0,
                  pool != nullptr ? pool : &pool_);
  tuple->SetValue(ColumnId::FINGERPRINT, val1, pool != nullptr ? pool : &pool_);
  tuple->SetValue(ColumnId::TIMESTAMP, val2, pool != nullptr ? pool : &pool_);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

}  // namespace catalog
}  // namespace peloton
