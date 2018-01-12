//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// query_history_catalog.cpp
//
// Identification: src/catalog/query_history_catalog.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/query_history_catalog.h"

#include "catalog/catalog.h"
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

QueryHistoryCatalog *QueryHistoryCatalog::GetInstance(
    concurrency::TransactionContext *txn) {
  static QueryHistoryCatalog query_history_catalog{txn};
  return &query_history_catalog;
}

QueryHistoryCatalog::QueryHistoryCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." QUERY_HISTORY_CATALOG_NAME
                      " ("
                      "query_string   VARCHAR NOT NULL, "
                      "fingerprint    VARCHAR NOT NULL, "
                      "timestamp      INT NOT NULL);",
                      txn) {}

QueryHistoryCatalog::~QueryHistoryCatalog() {}

bool QueryHistoryCatalog::InsertQueryHistory(const std::string &query_string, 
                                        std::string &fingerprint,
                                        int64_t timestamp,
                                        type::AbstractPool *pool, 
                                        concurrency::TransactionContext *txn) {

  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val0 = type::ValueFactory::GetVarcharValue(query_string, pool);
  auto val1 = type::ValueFactory::GetVarcharValue(fingerprint, pool);
  auto val2 = type::ValueFactory::GetIntegerValue(timestamp);

  tuple->SetValue(ColumnId::QUERY_STRING, val0, pool);
  tuple->SetValue(ColumnId::FINGERPRINT, val1, pool);
  tuple->SetValue(ColumnId::TIMESTAMP, val2, pool);

  // Insert the tuple
  return InsertTuple(std::move(tuple), txn);
}

}  // namespace catalog
}  // namespace peloton
