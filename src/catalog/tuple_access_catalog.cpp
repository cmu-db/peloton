//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metrics_catalog.cpp
//
// Identification: src/catalog/tuple_access_catalog.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/tuple_access_catalog.h"

#include <vector>
#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TupleAccessMetricsCatalogObject::TupleAccessMetricsCatalogObject(executor::LogicalTile *tile,
                                                                 oid_t tupleId)
    : tid_(tile->GetValue(tupleId, TupleAccessMetricsCatalog::ColumnId::TXN_ID).GetAs<txn_id_t>()),
      reads_(tile->GetValue(tupleId,
                         TupleAccessMetricsCatalog::ColumnId::READS)
              .GetAs<uint64_t>()),
      valid_(
          tile->GetValue(tupleId, TupleAccessMetricsCatalog::ColumnId::VALID)
              .GetAs<bool>()),
      committed_(
          tile->GetValue(tupleId,
                         TupleAccessMetricsCatalog::ColumnId::COMMITTED)
              .GetAs<bool>()) {}

TupleAccessMetricsCatalog::TupleAccessMetricsCatalog(concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE "  CATALOG_DATABASE_NAME
                      "." CATALOG_SCHEMA_NAME "." TUPLE_ACCESS_METRICS_CATALOG_NAME
                      " ("
                      "txn_id      BIGINT NOT NULL PRIMARY KEY, "
                      "reads       BIGINT NOT NULL, "
                      "valid       BOOL NOT NULL, "
                      "committed   BOOL NOT NULL);",
                      txn) {}

bool TupleAccessMetricsCatalog::InsertAccessMetric(peloton::txn_id_t tid,
                                                   uint64_t reads,
                                                   bool valid,
                                                   bool committed,
                                                   peloton::type::AbstractPool *pool,
                                                   peloton::concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple>
      tuple(new storage::Tuple(catalog_table_->GetSchema(), true));

  tuple->SetValue(ColumnId::TXN_ID,
                  type::ValueFactory::GetBigIntValue(tid),
                  pool);
  tuple->SetValue(ColumnId::READS,
                  type::ValueFactory::GetBigIntValue(reads),
                  pool);
  tuple->SetValue(ColumnId::VALID,
                  type::ValueFactory::GetBooleanValue(valid),
                  pool);
  tuple->SetValue(ColumnId::COMMITTED,
                  type::ValueFactory::GetBooleanValue(committed),
                  pool);
  return InsertTuple(std::move(tuple), txn);
}

bool TupleAccessMetricsCatalog::DeleteAccessMetrics(peloton::txn_id_t tid,
                                                    peloton::concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  return DeleteWithIndexScan(index_offset, values, txn);
}

bool TupleAccessMetricsCatalog::UpdateAccessMetrics(peloton::txn_id_t tid,
                                                    uint64_t reads,
                                                    bool valid,
                                                    bool committed,
                                                    peloton::concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns(all_column_ids_);
  std::vector<type::Value> update_values;
  update_values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  update_values.push_back(type::ValueFactory::GetBigIntValue(reads).Copy());
  update_values.push_back(type::ValueFactory::GetBooleanValue(valid).Copy());
  update_values.push_back(type::ValueFactory::GetBooleanValue(committed).Copy());

  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  oid_t index_offset = IndexId::PRIMARY_KEY;
  return UpdateWithIndexScan(update_columns,
                             update_values,
                             scan_values,
                             index_offset,
                             txn);
}

std::shared_ptr<TupleAccessMetricsCatalogObject> TupleAccessMetricsCatalog::GetTupleAccessMetricsCatalogObject(
    txn_id_t tid,
    concurrency::TransactionContext *txn) {
  if (txn == nullptr) throw CatalogException("Invalid Transaction");

  std::vector<oid_t> column_ids(all_column_ids_);
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);
  if (result_tiles->size() == 1 && (*result_tiles)[0]->GetTupleCount() == 1)
    return std::make_shared<TupleAccessMetricsCatalogObject>((*result_tiles)[0].get());
  return nullptr;
}
}  // namespace catalog
}  // namespace peloton
