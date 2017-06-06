//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_storage.cpp
//
// Identification: src/optimizer/stats/stats_storage.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/stats/stats_storage.h"
#include "catalog/catalog.h"
#include "catalog/column_stats_catalog.h"
#include "type/value.h"
#include "storage/data_table.h"

namespace peloton {
namespace optimizer {

// Get instance of the global stats storage
StatsStorage *StatsStorage::GetInstance(void) {
  static std::unique_ptr<StatsStorage> global_stats_storage(new StatsStorage());
  return global_stats_storage.get();
}

/**
 * StatsStorage - Constructor of StatsStorage.
 * In the construcotr, `stats` table and `samples_db` database are created.
 */
StatsStorage::StatsStorage() {
  pool_.reset(new type::EphemeralPool());
  CreateStatsCatalog();
}

/**
 * CreateStatsCatalog - Create 'stats' table in the catalog database.
 */
void StatsStorage::CreateStatsCatalog() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::ColumnStatsCatalog::GetInstance(txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * InsertOrUpdateTableStats - Add or update all column stats of a table.
 * This function iterates all column stats in the table stats and insert column
 * stats tuples into the 'stats' table in the catalog database.
 */
void StatsStorage::InsertOrUpdateTableStats(storage::DataTable *table,
                                            TableStats *table_stats,
                                            concurrency::Transaction *txn) {
  // Add or update column stats sequentially.
  oid_t database_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_row = table_stats->GetActiveTupleCount();

  oid_t column_count = table_stats->GetColumnCount();
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    ColumnStats *column_stats = table_stats->GetColumnStats(column_id);
    double cardinality = column_stats->GetCardinality();
    double frac_null = column_stats->GetFracNull();
    // Currently, we only store the most common value and its frequency in stats
    // table because Peloton doesn't support ARRAY type now.
    // TODO: Store multiple common values and freqs in stats table.
    std::vector<ValueFrequencyPair> most_common_val_freqs =
        column_stats->GetCommonValueAndFrequency();
    std::vector<double> histogram_bounds = column_stats->GetHistogramBound();

    std::string most_common_vals_str, most_common_freqs_str,
        histogram_bounds_str;

    auto val_freq_str = ConvertValueFreqArrayToStrings(most_common_val_freqs);
    most_common_vals_str = val_freq_str.first;
    most_common_freqs_str = val_freq_str.second;

    histogram_bounds_str = ConvertDoubleArrayToString(histogram_bounds);

    InsertOrUpdateColumnStats(database_id, table_id, column_id, num_row,
                              cardinality, frac_null, most_common_vals_str,
                              most_common_freqs_str, histogram_bounds_str, txn);
  }
}

void StatsStorage::InsertOrUpdateColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id, int num_row,
    double cardinality, double frac_null, std::string most_common_vals,
    std::string most_common_freqs, std::string histogram_bounds,
    concurrency::Transaction *txn) {
  LOG_DEBUG("InsertOrUpdateColumnStats, %d, %lf, %lf, %s, %s, %s", num_row,
            cardinality, frac_null, most_common_vals.c_str(),
            most_common_freqs.c_str(), histogram_bounds.c_str());
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  bool single_statement_txn = false;
  if (txn == nullptr) {
    single_statement_txn = true;
    txn = txn_manager.BeginTransaction();
  }
  column_stats_catalog->DeleteColumnStats(database_id, table_id, column_id,
                                          txn);
  column_stats_catalog->InsertColumnStats(
      database_id, table_id, column_id, num_row, cardinality, frac_null,
      most_common_vals, most_common_freqs, histogram_bounds, pool_.get(), txn);

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

/**
 * GetColumnStatsByID - Query the 'stats' table to get the column stats by IDs.
 */
std::unique_ptr<ColumnStatsSet> StatsStorage::GetColumnStatsByID(
    oid_t database_id, oid_t table_id, oid_t column_id) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  auto column_stats = column_stats_catalog->GetColumnStats(
      database_id, table_id, column_id, txn);
  txn_manager.CommitTransaction(txn);

  if (column_stats == nullptr) {
    LOG_TRACE("ColumnStats not found for db: %u, table: %u, column: %u",
              database_id, table_id, column_id);
    return nullptr;
  }
  int num_row = (*column_stats)[0].GetAs<int>();
  double cardinality = (*column_stats)[1].GetAs<double>();
  double frac_null = (*column_stats)[2].GetAs<double>();
  std::vector<double> val_array, freq_array, histogram_bounds;
  char *val_array_ptr = (*column_stats)[3].GetAs<char *>();
  if (val_array_ptr != nullptr) {
    val_array = ConvertStringToDoubleArray(std::string(val_array_ptr));
  }
  char *freq_array_ptr = (*column_stats)[4].GetAs<char *>();
  if (freq_array_ptr != nullptr) {
    freq_array = ConvertStringToDoubleArray(std::string(freq_array_ptr));
  }
  char *hist_bounds_ptr = (*column_stats)[5].GetAs<char *>();
  if (hist_bounds_ptr != nullptr) {
    LOG_TRACE("histgram bounds: %s", hist_bounds_ptr);
    histogram_bounds = ConvertStringToDoubleArray(std::string(hist_bounds_ptr));
  }
  std::unique_ptr<ColumnStatsSet> column_stats_set(
      new ColumnStatsSet(num_row, cardinality, frac_null, val_array, freq_array,
                         histogram_bounds));

  return std::move(column_stats_set);
}

/**
 * CollectStatsForAllTables - This function iterates all databases and
 * datatables
 * to collect their stats and store them in the 'stats' table.
 */
ResultType StatsStorage::AnalyzeStatsForAllTables(
    concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to analyze all tables' stats: %s");
    return ResultType::FAILURE;
  }

  auto catalog = catalog::Catalog::GetInstance();

  oid_t database_count = catalog->GetDatabaseCount();
  LOG_DEBUG("Database count: %u", database_count);
  for (oid_t db_offset = 0; db_offset < database_count; db_offset++) {
    auto database =
        catalog::Catalog::GetInstance()->GetDatabaseWithOffset(db_offset);
    if (database->GetDBName().compare(CATALOG_DATABASE_NAME) == 0) {
      continue;
    }
    oid_t table_count = database->GetTableCount();
    for (oid_t table_offset = 0; table_offset < table_count; table_offset++) {
      auto table = database->GetTable(table_offset);
      LOG_DEBUG("analyzing table: %s", table->GetName().c_str());
      std::unique_ptr<TableStats> table_stats(new TableStats(table));
      table_stats->CollectColumnStats();
      InsertOrUpdateTableStats(table, table_stats.get(), txn);
    }
  }
  return ResultType::SUCCESS;
}

ResultType StatsStorage::AnalyzeStatsForTable(storage::DataTable *table,
                                              concurrency::Transaction *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to analyze the table stats: %s",
              table_name.c_str());
    return ResultType::FAILURE;
  }
  std::unique_ptr<TableStats> table_stats(new TableStats(table));
  table_stats->CollectColumnStats();
  InsertOrUpdateTableStats(table, table_stats.get(), txn);
  return ResultType::SUCCESS;
}

// void StatsStorage::AnalayzeStatsForColumns(
//               UNUSED_ATTRIBUTE std::string database_name,
//               UNUSED_ATTRIBUTE std::string table_name,
//               UNUSED_ATTRIBUTE std::vector<std::string> column_names) {

// }

} /* namespace optimizer */
} /* namespace peloton */
