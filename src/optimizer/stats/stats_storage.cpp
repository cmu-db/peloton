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
#include "concurrency/transaction_manager_factory.h"
#include "optimizer/stats/column_stats.h"
#include "optimizer/stats/table_stats.h"
#include "storage/storage_manager.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace optimizer {

// Get instance of the global stats storage
StatsStorage *StatsStorage::GetInstance() {
  static StatsStorage global_stats_storage;
  return &global_stats_storage;
}

/**
 * StatsStorage - Constructor of StatsStorage.
 * In the construcotr, `pg_column_stats` table and `samples_db` database are
 * created.
 */
StatsStorage::StatsStorage() {
  pool_.reset(new type::EphemeralPool());
  CreateStatsTableInCatalog();
}

/**
 * CreateStatsCatalog - Create 'pg_column_stats' table in the catalog database.
 */
void StatsStorage::CreateStatsTableInCatalog() {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  catalog::ColumnStatsCatalog::GetInstance(txn);
  txn_manager.CommitTransaction(txn);
}

/**
 * InsertOrUpdateTableStats - Add or update all column stats of a table.
 * This function iterates all column stats in the table stats and insert column
 * stats tuples into the 'pg_column_stats' table in the catalog database.
 */
void StatsStorage::InsertOrUpdateTableStats(
    storage::DataTable *table, TableStatsCollector *table_stats_collector,
    concurrency::TransactionContext *txn) {
  // Add or update column stats sequentially.
  oid_t database_id = table->GetDatabaseOid();
  oid_t table_id = table->GetOid();
  size_t num_rows = table_stats_collector->GetActiveTupleCount();

  oid_t column_count = table_stats_collector->GetColumnCount();
  for (oid_t column_id = 0; column_id < column_count; column_id++) {
    ColumnStatsCollector *column_stats_collector =
        table_stats_collector->GetColumnStats(column_id);
    double cardinality = column_stats_collector->GetCardinality();
    double frac_null = column_stats_collector->GetFracNull();
    // Currently, we only store the most common <value, frequency> pairs for
    // numeric values.
    // Since Peloton doesn't support ARRAY type now, we conver the array to
    // VARCHAR.
    // TODO: Store common <value, freq> pairs for VARCHAR.
    std::vector<ValueFrequencyPair> most_common_val_freqs =
        column_stats_collector->GetCommonValueAndFrequency();
    std::vector<double> histogram_bounds =
        column_stats_collector->GetHistogramBound();

    std::string most_common_vals_str, most_common_freqs_str,
        histogram_bounds_str;

    auto val_freq_str = ConvertValueFreqArrayToStrings(most_common_val_freqs);
    most_common_vals_str = val_freq_str.first;
    most_common_freqs_str = val_freq_str.second;

    histogram_bounds_str = ConvertDoubleArrayToString(histogram_bounds);

    std::string column_name = column_stats_collector->GetColumnName();
    bool has_index = column_stats_collector->HasIndex();

    InsertOrUpdateColumnStats(database_id, table_id, column_id, num_rows,
                              cardinality, frac_null, most_common_vals_str,
                              most_common_freqs_str, histogram_bounds_str,
                              column_name, has_index, txn);
  }
}

/**
 * InsertOrUpdateColumnStats - Insert or update a column stats.
 */
void StatsStorage::InsertOrUpdateColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id, int num_rows,
    double cardinality, double frac_null, std::string most_common_vals,
    std::string most_common_freqs, std::string histogram_bounds,
    std::string column_name, bool has_index, concurrency::TransactionContext *txn) {
  LOG_TRACE("InsertOrUpdateColumnStats, %d, %lf, %lf, %s, %s, %s", num_rows,
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
      database_id, table_id, column_id, num_rows, cardinality, frac_null,
      most_common_vals, most_common_freqs, histogram_bounds, column_name,
      has_index, pool_.get(), txn);

  if (single_statement_txn) {
    txn_manager.CommitTransaction(txn);
  }
}

/**
 * GetColumnStatsByID - Query the 'pg_column_stats' table to get the column
 * stats by IDs.
 */
std::shared_ptr<ColumnStats> StatsStorage::GetColumnStatsByID(oid_t database_id,
                                                              oid_t table_id,
                                                              oid_t column_id) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  // std::unique_ptr<std::vector<type::Value>> column_stats_vector
  auto column_stats_vector = column_stats_catalog->GetColumnStats(
      database_id, table_id, column_id, txn);
  txn_manager.CommitTransaction(txn);

  return ConvertVectorToColumnStats(database_id, table_id, column_id,
                                    column_stats_vector);
}

/**
 * ConvertVectorToColumnStats - It's a helper function to convert the vector of
 * type::Value to
 * the object ColumnStats and return the shared pointer to it.
 */
std::shared_ptr<ColumnStats> StatsStorage::ConvertVectorToColumnStats(
    oid_t database_id, oid_t table_id, oid_t column_id,
    std::unique_ptr<std::vector<type::Value>> &column_stats_vector) {
  if (column_stats_vector == nullptr) {
    LOG_TRACE(
        "ColumnStatsCollector not found for db: %u, table: %u, column: %u",
        database_id, table_id, column_id);
    return nullptr;
  }

  int num_rows =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::NUM_ROWS_OFF]
          .GetAs<int>();
  double cardinality =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::CARDINALITY_OFF]
          .GetAs<double>();
  double frac_null =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::FRAC_NULL_OFF]
          .GetAs<double>();

  std::vector<double> val_array, freq_array, histogram_bounds;
  char *val_array_ptr =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::COMMON_VALS_OFF]
          .GetAs<char *>();
  if (val_array_ptr != nullptr) {
    val_array = ConvertStringToDoubleArray(std::string(val_array_ptr));
  }
  char *freq_array_ptr =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::COMMON_FREQS_OFF]
          .GetAs<char *>();
  if (freq_array_ptr != nullptr) {
    freq_array = ConvertStringToDoubleArray(std::string(freq_array_ptr));
  }
  char *hist_bounds_ptr =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::HIST_BOUNDS_OFF]
          .GetAs<char *>();
  if (hist_bounds_ptr != nullptr) {
    LOG_TRACE("histgram bounds: %s", hist_bounds_ptr);
    histogram_bounds = ConvertStringToDoubleArray(std::string(hist_bounds_ptr));
  }

  char *column_name =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::COLUMN_NAME_OFF]
          .GetAs<char *>();

  bool has_index =
      (*column_stats_vector)[catalog::ColumnStatsCatalog::HAS_INDEX_OFF]
          .GetAs<bool>();

  std::shared_ptr<ColumnStats> column_stats(new ColumnStats(
      database_id, table_id, column_id, std::string(column_name), has_index,
      num_rows, cardinality, frac_null, val_array, freq_array,
      histogram_bounds));

  return column_stats;
}

/**
 * GetTableStats - This function queries the column_stats_catalog for the table
 *stats.
 *
 * The return value is the shared_ptr of TableStats wrapper.
 */
std::shared_ptr<TableStats> StatsStorage::GetTableStats(oid_t database_id,
                                                        oid_t table_id) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::map<oid_t, std::unique_ptr<std::vector<type::Value>>> column_stats_map;
  column_stats_catalog->GetTableStats(database_id, table_id, txn,
                                      column_stats_map);
  txn_manager.CommitTransaction(txn);

  std::vector<std::shared_ptr<ColumnStats>> column_stats_ptrs;
  for (auto it = column_stats_map.begin(); it != column_stats_map.end(); ++it) {
    column_stats_ptrs.push_back(ConvertVectorToColumnStats(
        database_id, table_id, it->first, it->second));
  }

  return std::shared_ptr<TableStats>(new TableStats(column_stats_ptrs));
}

/**
 * GetTableStats - This function query the column_stats_catalog for the table
 *stats.
 * In this function, the column ids are specified.
 *
 * The return value is the shared_ptr of TableStats wrapper.
 */
std::shared_ptr<TableStats> StatsStorage::GetTableStats(
    oid_t database_id, oid_t table_id, std::vector<oid_t> column_ids) {
  auto column_stats_catalog = catalog::ColumnStatsCatalog::GetInstance(nullptr);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();
  std::map<oid_t, std::unique_ptr<std::vector<type::Value>>> column_stats_map;
  column_stats_catalog->GetTableStats(database_id, table_id, txn,
                                      column_stats_map);

  std::vector<std::shared_ptr<ColumnStats>> column_stats_ptrs;
  for (oid_t col_id : column_ids) {
    auto it = column_stats_map.find(col_id);
    if (it != column_stats_map.end()) {
      column_stats_ptrs.push_back(ConvertVectorToColumnStats(
          database_id, table_id, col_id, it->second));
    }
  }

  return std::shared_ptr<TableStats>(new TableStats(column_stats_ptrs));
}

/**
 * AnalyzeStatsForAllTables - This function iterates all databases and
 * datatables to collect their stats and store them in the column_stats_catalog.
 */
ResultType StatsStorage::AnalyzeStatsForAllTables(
    concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to analyze all tables' stats.");
    return ResultType::FAILURE;
  }

  auto storage_manager = storage::StorageManager::GetInstance();

  oid_t database_count = storage_manager->GetDatabaseCount();
  LOG_TRACE("Database count: %u", database_count);
  for (oid_t db_offset = 0; db_offset < database_count; db_offset++) {
    auto database = storage_manager->GetDatabaseWithOffset(db_offset);
    if (database->GetOid() == CATALOG_DATABASE_OID) {
      continue;
    }
    oid_t table_count = database->GetTableCount();
    for (oid_t table_offset = 0; table_offset < table_count; table_offset++) {
      auto table = database->GetTable(table_offset);
      LOG_TRACE("Analyzing table: %s", table->GetName().c_str());
      std::unique_ptr<TableStatsCollector> table_stats_collector(
          new TableStatsCollector(table));
      table_stats_collector->CollectColumnStats();
      InsertOrUpdateTableStats(table, table_stats_collector.get(), txn);
    }
  }
  return ResultType::SUCCESS;
}

/**
 * AnalyzeStatsForTable - This function analyzes the stats for one table and
 * sotre the stats in column_stats_catalog.
 */
ResultType StatsStorage::AnalyzeStatsForTable(storage::DataTable *table,
                                              concurrency::TransactionContext *txn) {
  if (txn == nullptr) {
    LOG_TRACE("Do not have transaction to analyze the table stats: %s",
              table->GetName().c_str());
    return ResultType::FAILURE;
  }
  std::unique_ptr<TableStatsCollector> table_stats_collector(
      new TableStatsCollector(table));
  table_stats_collector->CollectColumnStats();
  InsertOrUpdateTableStats(table, table_stats_collector.get(), txn);
  return ResultType::SUCCESS;
}

// TODO: Implement it.
ResultType StatsStorage::AnalayzeStatsForColumns(
    UNUSED_ATTRIBUTE storage::DataTable *table,
    UNUSED_ATTRIBUTE std::vector<std::string> column_names) {
  return ResultType::FAILURE;
}

}  // namespace optimizer
}  // namespace peloton
