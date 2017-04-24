//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats_catalog.cpp
//
// Identification: src/catalog/column_stats_catalog.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "catalog/catalog.h"
#include "catalog/column_stats_catalog.h"
#include "optimizer/stats/column_stats.h"
#include "type/type.h"

namespace peloton {
namespace catalog {

ColumnStatsCatalog *ColumnStatsCatalog::GetInstance(
    concurrency::Transaction *txn) {
  static std::unique_ptr<ColumnStatsCatalog> column_stats_catalog(
      new ColumnStatsCatalog(txn));

  return column_stats_catalog.get();
}

ColumnStatsCatalog::ColumnStatsCatalog(concurrency::Transaction *txn)
    : AbstractCatalog("CREATE TABLE " CATALOG_DATABASE_NAME
                      "." COLUMN_STATS_CATALOG_NAME
                      " ("
                      "database_id    INT NOT NULL, "
                      "table_id       INT NOT NULL, "
                      "column_id      INT NOT NULL, "
                      "num_row        INT NOT NULL, "
                      "cardinality    DECIMAL NOT NULL, "
                      "frac_null      DECIMAL NOT NULL, "
                      "most_common_vals  VARCHAR, "
                      "most_common_freqs DECIMAL, "
                      "histogram_bounds  VARCHAR);",
                      txn) {
  // Add secondary index here if necessary
  Catalog::GetInstance()->CreateIndex(
      CATALOG_DATABASE_NAME, COLUMN_STATS_CATALOG_NAME,
      {"database_id", "table_id", "column_id"},
      COLUMN_STATS_CATALOG_NAME "_skey0", false, IndexType::BWTREE, txn);
}

ColumnStatsCatalog::~ColumnStatsCatalog() {}

bool ColumnStatsCatalog::InsertColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id, int num_row,
    double cardinality, double frac_null, std::string most_common_vals,
    double most_common_freqs, std::string histogram_bounds,
    type::AbstractPool *pool, concurrency::Transaction *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val_db_id = type::ValueFactory::GetIntegerValue(database_id);
  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_num_row = type::ValueFactory::GetIntegerValue(num_row);
  auto val_cardinality = type::ValueFactory::GetDecimalValue(cardinality);
  auto val_frac_null = type::ValueFactory::GetDecimalValue(frac_null);

  type::Value val_common_val, val_common_freq;
  if (!most_common_vals.empty()) {
    val_common_val = type::ValueFactory::GetVarcharValue(most_common_vals);
    val_common_freq = type::ValueFactory::GetDecimalValue(most_common_freqs);
  } else {
    val_common_val =
        type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
    val_common_freq =
        type::ValueFactory::GetNullValueByType(type::Type::DECIMAL);
  }

  type::Value val_hist_bounds;
  if (!histogram_bounds.empty()) {
    val_hist_bounds = type::ValueFactory::GetVarcharValue(histogram_bounds);
  } else {
    val_hist_bounds =
        type::ValueFactory::GetNullValueByType(type::Type::VARCHAR);
  }

  tuple->SetValue(ColumnId::DATABASE_ID, val_db_id, nullptr);
  tuple->SetValue(ColumnId::TABLE_ID, val_table_id, nullptr);
  tuple->SetValue(ColumnId::COLUMN_ID, val_column_id, nullptr);
  tuple->SetValue(ColumnId::NUM_ROW, val_num_row, nullptr);
  tuple->SetValue(ColumnId::CARDINALITY, val_cardinality, nullptr);
  tuple->SetValue(ColumnId::FRAC_NULL, val_frac_null, nullptr);
  tuple->SetValue(ColumnId::MOST_COMMON_VALS, val_common_val, pool);
  tuple->SetValue(ColumnId::MOST_COMMON_FREQS, val_common_freq, nullptr);
  tuple->SetValue(ColumnId::HISTOGRAM_BOUNDS, val_hist_bounds, pool);

  // Insert the tuple into catalog table
  return InsertTuple(std::move(tuple), txn);
}

bool ColumnStatsCatalog::DeleteColumnStats(oid_t database_id, oid_t table_id,
                                           oid_t column_id,
                                           concurrency::Transaction *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<optimizer::ColumnStats> ColumnStatsCatalog::GetColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id,
    concurrency::Transaction *txn) {
  std::vector<oid_t> column_ids(
      {ColumnId::NUM_ROW, ColumnId::CARDINALITY, ColumnId::FRAC_NULL});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(database_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  std::unique_ptr<optimizer::ColumnStats> column_stats(
      new optimizer::ColumnStats(database_id, table_id, column_id,
                                 type::Type::INTEGER));
  PL_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() != 0) {
    PL_ASSERT((*result_tiles)[0]->GetTupleCount() <= 1);
    if ((*result_tiles)[0]->GetTupleCount() != 0) {
      auto num_row = (*result_tiles)[0]->GetValue(0, 0);
      auto cardinality = (*result_tiles)[0]->GetValue(0, 1);
      auto frac_null = (*result_tiles)[0]->GetValue(0, 2);
    }
  }

  return std::move(column_stats);
}

}  // End catalog namespace
}  // End peloton namespace
