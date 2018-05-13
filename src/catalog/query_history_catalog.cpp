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
#include "executor/logical_tile.h"

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
                      txn) {
  // Secondary index on timestamp
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, CATALOG_SCHEMA_NAME, QUERY_HISTORY_CATALOG_NAME,
      {2}, QUERY_HISTORY_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

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

std::unique_ptr<std::vector<std::pair<uint64_t, std::string>>>
QueryHistoryCatalog::GetQueryStringsAfterTimestamp(
    const uint64_t start_timestamp, concurrency::TransactionContext *txn) {
  LOG_INFO("Start querying.... %" PRId64, start_timestamp);
  // Get both timestamp and query string in the result.
  std::vector<oid_t> column_ids({ColumnId::TIMESTAMP, ColumnId::QUERY_STRING});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetTimestampValue(
      static_cast<uint64_t>(start_timestamp)));

  std::vector<ExpressionType> expr_types(values.size(),
                                         ExpressionType::COMPARE_GREATERTHAN);

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, expr_types, txn);

  std::unique_ptr<std::vector<std::pair<uint64_t, std::string>>> queries(
      new std::vector<std::pair<uint64_t, std::string>>());
  if (result_tiles->size() > 0) {
    for (auto &tile : *result_tiles.get()) {
      PELOTON_ASSERT(tile->GetColumnCount() == column_ids.size());
      for (auto i = 0UL; i < tile->GetTupleCount(); i++) {
        auto timestamp = tile->GetValue(i, 0).GetAs<uint64_t>();
        auto query_string = tile->GetValue(i, 1).GetAs<char *>();
        auto pair = std::make_pair(timestamp, query_string);
        LOG_INFO("Query: %" PRId64 ": %s", pair.first, pair.second);
        queries->emplace_back(pair);
      }
    }
  }
  return queries;
}

}  // namespace catalog
}  // namespace peloton
