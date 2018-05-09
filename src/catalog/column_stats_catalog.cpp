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

#include "catalog/column_stats_catalog.h"

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "executor/logical_tile.h"
#include "optimizer/stats/column_stats_collector.h"
#include "storage/data_table.h"
#include "storage/tuple.h"

namespace peloton {
namespace catalog {

ColumnStatsCatalog::ColumnStatsCatalog(
        storage::Database *pg_catalog,
        UNUSED_ATTRIBUTE type::AbstractPool *pool,
        UNUSED_ATTRIBUTE concurrency::TransactionContext *txn)
        : AbstractCatalog(COLUMN_STATS_CATALOG_OID, COLUMN_STATS_CATALOG_NAME,
                          InitializeSchema().release(), pg_catalog) {
  // Add indexes for pg_column_stats
  AddIndex({ColumnId::TABLE_ID, ColumnId::COLUMN_ID},
           COLUMN_STATS_CATALOG_SKEY0_OID, COLUMN_STATS_CATALOG_NAME "_skey0",
           IndexConstraintType::UNIQUE);
  AddIndex({ColumnId::TABLE_ID}, COLUMN_STATS_CATALOG_SKEY1_OID,
           COLUMN_STATS_CATALOG_NAME "_skey1", IndexConstraintType::DEFAULT);

}

ColumnStatsCatalog::~ColumnStatsCatalog() {}

std::unique_ptr<catalog::Schema> ColumnStatsCatalog::InitializeSchema() {

  const std::string not_null_constraint_name = "notnull";
  const auto not_null_constraint = catalog::Constraint(
         ConstraintType::NOTNULL, not_null_constraint_name);

  auto table_id_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "table_id", true);
  table_id_column.AddConstraint(not_null_constraint);
  auto column_id_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "column_id", true);
  column_id_column.AddConstraint(not_null_constraint);
  auto num_rows_column = catalog::Column(
          type::TypeId::INTEGER, type::Type::GetTypeSize(type::TypeId::INTEGER),
          "num_rows", true);
  num_rows_column.AddConstraint(not_null_constraint);
  auto cardinality_column = catalog::Column(
          type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
          "cardinality", true);
  cardinality_column.AddConstraint(not_null_constraint);
  auto frac_null_column = catalog::Column(
          type::TypeId::DECIMAL, type::Type::GetTypeSize(type::TypeId::DECIMAL),
          "frac_null", true);
  frac_null_column.AddConstraint(not_null_constraint);
  auto most_common_vals_column = catalog::Column(
          type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
          "most_common_vals", false);
  auto most_common_freqs_column = catalog::Column(
          type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
          "most_common_freqs", false);
  auto histogram_bounds_column = catalog::Column(
          type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
          "histogram_bounds", false);
  auto column_name_column = catalog::Column(
          type::TypeId::VARCHAR, type::Type::GetTypeSize(type::TypeId::VARCHAR),
          "column_name", false);
  auto has_index_column = catalog::Column(
          type::TypeId::BOOLEAN, type::Type::GetTypeSize(type::TypeId::BOOLEAN),
          "has_index", true);

  std::unique_ptr<catalog::Schema> column_stats_schema(new catalog::Schema(
          {table_id_column, column_id_column, num_rows_column, cardinality_column,
           frac_null_column, most_common_vals_column, most_common_freqs_column,
           histogram_bounds_column, column_name_column, has_index_column}));
  return column_stats_schema;
}

bool ColumnStatsCatalog::InsertColumnStats(
    oid_t table_id, oid_t column_id, int num_rows,
    double cardinality, double frac_null, std::string most_common_vals,
    std::string most_common_freqs, std::string histogram_bounds,
    std::string column_name, bool has_index, type::AbstractPool *pool,
    concurrency::TransactionContext *txn) {
  std::unique_ptr<storage::Tuple> tuple(
      new storage::Tuple(catalog_table_->GetSchema(), true));

  auto val_table_id = type::ValueFactory::GetIntegerValue(table_id);
  auto val_column_id = type::ValueFactory::GetIntegerValue(column_id);
  auto val_num_row = type::ValueFactory::GetIntegerValue(num_rows);
  auto val_cardinality = type::ValueFactory::GetDecimalValue(cardinality);
  auto val_frac_null = type::ValueFactory::GetDecimalValue(frac_null);

  type::Value val_common_val, val_common_freq;
  if (!most_common_vals.empty()) {
    val_common_val = type::ValueFactory::GetVarcharValue(most_common_vals);
    val_common_freq = type::ValueFactory::GetVarcharValue(most_common_freqs);
  } else {
    val_common_val =
        type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    val_common_freq =
        type::ValueFactory::GetNullValueByType(type::TypeId::DECIMAL);
  }

  type::Value val_hist_bounds;
  if (!histogram_bounds.empty()) {
    val_hist_bounds = type::ValueFactory::GetVarcharValue(histogram_bounds);
  } else {
    val_hist_bounds =
        type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  type::Value val_column_name =
      type::ValueFactory::GetVarcharValue(column_name);
  type::Value val_has_index = type::ValueFactory::GetBooleanValue(has_index);

  tuple->SetValue(ColumnId::TABLE_ID, val_table_id, nullptr);
  tuple->SetValue(ColumnId::COLUMN_ID, val_column_id, nullptr);
  tuple->SetValue(ColumnId::NUM_ROWS, val_num_row, nullptr);
  tuple->SetValue(ColumnId::CARDINALITY, val_cardinality, nullptr);
  tuple->SetValue(ColumnId::FRAC_NULL, val_frac_null, nullptr);
  tuple->SetValue(ColumnId::MOST_COMMON_VALS, val_common_val, pool);
  tuple->SetValue(ColumnId::MOST_COMMON_FREQS, val_common_freq, pool);
  tuple->SetValue(ColumnId::HISTOGRAM_BOUNDS, val_hist_bounds, pool);
  tuple->SetValue(ColumnId::COLUMN_NAME, val_column_name, pool);
  tuple->SetValue(ColumnId::HAS_INDEX, val_has_index, nullptr);

  // Insert the tuple into catalog table
  return InsertTuple(std::move(tuple), txn);
}

bool ColumnStatsCatalog::DeleteColumnStats(
        oid_t table_id, oid_t column_id,
        concurrency::TransactionContext *txn) {
  oid_t index_offset = IndexId::SECONDARY_KEY_0;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  return DeleteWithIndexScan(index_offset, values, txn);
}

std::unique_ptr<std::vector<type::Value>> ColumnStatsCatalog::GetColumnStats(
    oid_t table_id, oid_t column_id,
    concurrency::TransactionContext *txn) {
  std::vector<oid_t> column_ids(
      {ColumnId::NUM_ROWS, ColumnId::CARDINALITY, ColumnId::FRAC_NULL,
       ColumnId::MOST_COMMON_VALS, ColumnId::MOST_COMMON_FREQS,
       ColumnId::HISTOGRAM_BOUNDS, ColumnId::COLUMN_NAME, ColumnId::HAS_INDEX});
  oid_t index_offset = IndexId::SECONDARY_KEY_0;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());
  values.push_back(type::ValueFactory::GetIntegerValue(column_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  PELOTON_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() == 0) {
    return nullptr;
  }

  auto tile = (*result_tiles)[0].get();
  PELOTON_ASSERT(tile->GetTupleCount() <= 1);
  if (tile->GetTupleCount() == 0) {
    return nullptr;
  }

  type::Value num_rows, cardinality, frac_null, most_common_vals,
      most_common_freqs, hist_bounds, column_name, has_index;

  num_rows = tile->GetValue(0, ColumnStatsOffset::NUM_ROWS_OFF);
  cardinality = tile->GetValue(0, ColumnStatsOffset::CARDINALITY_OFF);
  frac_null = tile->GetValue(0, ColumnStatsOffset::FRAC_NULL_OFF);
  most_common_vals = tile->GetValue(0, ColumnStatsOffset::COMMON_VALS_OFF);
  most_common_freqs = tile->GetValue(0, ColumnStatsOffset::COMMON_FREQS_OFF);
  hist_bounds = tile->GetValue(0, ColumnStatsOffset::HIST_BOUNDS_OFF);
  column_name = tile->GetValue(0, ColumnStatsOffset::COLUMN_NAME_OFF);
  has_index = tile->GetValue(0, ColumnStatsOffset::HAS_INDEX_OFF);

  std::unique_ptr<std::vector<type::Value>> column_stats(
      new std::vector<type::Value>({num_rows, cardinality, frac_null,
                                    most_common_vals, most_common_freqs,
                                    hist_bounds, column_name, has_index}));

  return column_stats;
}

// Return value: number of column stats
size_t ColumnStatsCatalog::GetTableStats(
    oid_t table_id, concurrency::TransactionContext *txn,
    std::map<oid_t, std::unique_ptr<std::vector<type::Value>>>
        &column_stats_map) {
  std::vector<oid_t> column_ids(
      {ColumnId::COLUMN_ID, ColumnId::NUM_ROWS, ColumnId::CARDINALITY,
       ColumnId::FRAC_NULL, ColumnId::MOST_COMMON_VALS,
       ColumnId::MOST_COMMON_FREQS, ColumnId::HISTOGRAM_BOUNDS,
       ColumnId::COLUMN_NAME, ColumnId::HAS_INDEX});
  oid_t index_offset = IndexId::SECONDARY_KEY_1;  // Secondary key index

  std::vector<type::Value> values;
  values.push_back(type::ValueFactory::GetIntegerValue(table_id).Copy());

  auto result_tiles =
      GetResultWithIndexScan(column_ids, index_offset, values, txn);

  PELOTON_ASSERT(result_tiles->size() <= 1);  // unique
  if (result_tiles->size() == 0) {
    return 0;
  }
  auto tile = (*result_tiles)[0].get();
  size_t tuple_count = tile->GetTupleCount();
  LOG_DEBUG("Tuple count: %lu", tuple_count);
  if (tuple_count == 0) {
    return 0;
  }

  type::Value num_rows, cardinality, frac_null, most_common_vals,
      most_common_freqs, hist_bounds, column_name, has_index;
  for (size_t tuple_id = 0; tuple_id < tuple_count; ++tuple_id) {
    num_rows = tile->GetValue(tuple_id, 1 + ColumnStatsOffset::NUM_ROWS_OFF);
    cardinality =
        tile->GetValue(tuple_id, 1 + ColumnStatsOffset::CARDINALITY_OFF);
    frac_null = tile->GetValue(tuple_id, 1 + ColumnStatsOffset::FRAC_NULL_OFF);
    most_common_vals =
        tile->GetValue(tuple_id, 1 + ColumnStatsOffset::COMMON_VALS_OFF);
    most_common_freqs =
        tile->GetValue(tuple_id, 1 + ColumnStatsOffset::COMMON_FREQS_OFF);
    hist_bounds =
        tile->GetValue(tuple_id, 1 + ColumnStatsOffset::HIST_BOUNDS_OFF);
    column_name =
        tile->GetValue(tuple_id, 1 + ColumnStatsOffset::COLUMN_NAME_OFF);
    has_index = tile->GetValue(tuple_id, 1 + ColumnStatsOffset::HAS_INDEX_OFF);

    std::unique_ptr<std::vector<type::Value>> column_stats(
        new std::vector<type::Value>({num_rows, cardinality, frac_null,
                                      most_common_vals, most_common_freqs,
                                      hist_bounds, column_name, has_index}));

    oid_t column_id = tile->GetValue(tuple_id, 0).GetAs<int>();
    column_stats_map[column_id] = std::move(column_stats);
  }
  return tuple_count;
}

}  // namespace catalog
}  // namespace peloton
