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

#include "executor/logical_tile.h"
#include "storage/data_table.h"
#include "type/value_factory.h"

namespace peloton {
namespace catalog {

TupleAccessMetricsCatalog::TupleAccessMetricsCatalog(const std::string &database_name,
                                                     concurrency::TransactionContext *txn)
    : AbstractCatalog("CREATE TABLE " + database_name +
                          "." CATALOG_SCHEMA_NAME "." TUPLE_ACCESS_METRICS_CATALOG_NAME
                          " ("
                          "txn_id      BIGINT NOT NULL, "
                          "arrival     BIGINT NOT NULL, "
                          "reads       BIGINT NOT NULL, "
                          "valid       BOOL NOT NULL, "
                          "committed   BOOL NOT NULL, "
                          "PRIMARY KEY(txn_id, arrival));",
                      txn) {}

bool TupleAccessMetricsCatalog::InsertAccessMetric(peloton::txn_id_t tid,
                                                   int64_t arrival,
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
  tuple->SetValue(ColumnId::ARRIVAL,
                  type::ValueFactory::GetBigIntValue(arrival),
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
                                                    int64_t arrival,
                                                    peloton::concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::PRIMARY_KEY;
  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  values.push_back(type::ValueFactory::GetBigIntValue(arrival).Copy());
  return DeleteWithIndexScan(index_offset, values, txn);
}

bool TupleAccessMetricsCatalog::UpdateAccessMetrics(peloton::txn_id_t tid,
                                                    int64_t arrival,
                                                    uint64_t reads,
                                                    bool valid,
                                                    bool committed,
                                                    peloton::concurrency::TransactionContext *txn) {
  std::vector<oid_t> update_columns(all_column_ids_);
  std::vector<type::Value> update_values;
  update_values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  update_values.push_back(type::ValueFactory::GetBigIntValue(arrival).Copy());
  update_values.push_back(type::ValueFactory::GetBigIntValue(reads).Copy());
  update_values.push_back(type::ValueFactory::GetBooleanValue(valid).Copy());
  update_values.push_back(type::ValueFactory::GetBooleanValue(committed).Copy());

  std::vector<type::Value> scan_values;
  scan_values.push_back(type::ValueFactory::GetBigIntValue(tid).Copy());
  scan_values.push_back(type::ValueFactory::GetBigIntValue(arrival).Copy());
  oid_t index_offset = IndexId::PRIMARY_KEY;
  return UpdateWithIndexScan(update_columns,
                             update_values,
                             scan_values,
                             index_offset,
                             txn);
}
}
}  // namespace catalog
}  // namespace peloton
