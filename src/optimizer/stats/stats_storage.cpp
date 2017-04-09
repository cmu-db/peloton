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
#include "type/value.h"
#include "storage/data_table.h"
#include "storage/table_factory.h"

namespace peloton {
namespace optimizer {

// Get instance of the global stats storage
StatsStorage *StatsStorage::GetInstance(void) {
  static std::unique_ptr<StatsStorage> global_stats_storage(new StatsStorage());
  return global_stats_storage.get();
}

StatsStorage::StatsStorage() {
  CreateStatsCatalog();
  CreateSamplesDatabase();
}

void StatsStorage::CreateStatsCatalog() {
  auto catalog = catalog::Catalog::GetInstance();
  auto catalog_db = catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  auto catalog_db_oid = catalog_db->GetOid();
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = true;

  // Create table for stats
  auto stats_schema = InitializeStatsSchema();
  std::unique_ptr<storage::DataTable> table(
        storage::TableFactory::GetDataTable(catalog_db_oid, catalog->GetNextOid(),
        stats_schema.release(), STATS_TABLE_NAME, DEFAULT_TUPLES_PER_TILEGROUP,
        own_schema, adapt_table, is_catalog));
  catalog_db->AddTable(table.release(), true);
}

std::unique_ptr<catalog::Schema> StatsStorage::InitializeStatsSchema() {
  const std::string not_null_constraint_name = "not_null";
  catalog::Constraint not_null_constraint(ConstraintType::NOTNULL,
      not_null_constraint_name);
  oid_t integer_type_size = type::Type::GetTypeSize(type::Type::INTEGER);
  type::Type::TypeId integer_type = type::Type::INTEGER;
  oid_t varchar_type_size = type::Type::GetTypeSize(type::Type::VARCHAR);
  type::Type::TypeId varchar_type = type::Type::VARCHAR;

  auto database_id_column = catalog::Column(integer_type, integer_type_size,
      "database_id", true);
  database_id_column.AddConstraint(not_null_constraint);
  auto table_id_column = catalog::Column(integer_type, integer_type_size,
      "table_id", true);
  table_id_column.AddConstraint(not_null_constraint);
  auto column_id_column = catalog::Column(integer_type, integer_type_size,
      "column_id", true);
  column_id_column.AddConstraint(not_null_constraint);

  auto num_row_column = catalog::Column(integer_type, integer_type_size,
      "num_row", true);
  num_row_column.AddConstraint(not_null_constraint);
  auto num_distinct_column = catalog::Column(integer_type, integer_type_size,
      "num_distinct", true);
  num_distinct_column.AddConstraint(not_null_constraint);
  auto num_null_column = catalog::Column(integer_type, integer_type_size,
      "num_null", true);
  num_null_column.AddConstraint(not_null_constraint);

  auto most_common_vals_column = catalog::Column(varchar_type, varchar_type_size,
      "most_common_vals_column");
//  most_common_vals_column.AddConstraint(not_null_constraint);

  auto most_common_freqs_column = catalog::Column(varchar_type, varchar_type_size,
      "most_common_freqs_column");
//  most_common_freqs_column.AddConstraint(not_null_constraint);

  auto histogram_bounds_column = catalog::Column(varchar_type, varchar_type_size,
      "histogram_bounds");
//  histogram_bounds_column.AddConstraint(not_null_constraint);

  std::unique_ptr<catalog::Schema> table_schema(new catalog::Schema( {
      database_id_column, table_id_column, column_id_column,
      num_row_column, num_distinct_column, num_null_column, most_common_vals_column,
      most_common_freqs_column, histogram_bounds_column }));
  return table_schema;
}

storage::DataTable *StatsStorage::GetStatsTable() {
  auto catalog = catalog::Catalog::GetInstance();
  storage::Database *catalog_db = catalog->GetDatabaseWithName(CATALOG_DATABASE_NAME);
  auto stats_table = catalog_db->GetTableWithName(STATS_TABLE_NAME);

  return stats_table;
}

void StatsStorage::StoreTableStats(TableStats *table_stats) {
  // All tuples are inserted in a single txn
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto txn = txn_manager.BeginTransaction();

  storage::DataTable *stats_table = GetStatsTable();

  oid_t database_id = table_stats->GetDatabaseID();
  oid_t table_id = table_stats->GetTableID();
  size_t num_row = table_stats->GetActiveTupleCount();

  oid_t column_count = table_stats->GetColumnCount();
  for(oid_t column_id = 0; column_id < column_count; column_id ++) {
    ColumnStats *column_stats = table_stats->GetColumnStats(column_id);
    (void) column_stats;
    int num_distinct = column_stats->GetDistinctCount();
    int num_null = column_stats->GetNullCount();
    // TODO: Get most_common_vals, most_common_freqs and histogram_bounds from column_stats.
    std::vector<type::Value> most_common_vals;
    std::vector<int> most_common_freqs;
    std::vector<double> histogram_bounds;

    // Generate and insert the tuple.
    auto table_tuple = GetColumnStatsTuple(stats_table->GetSchema(),
                                    database_id, table_id, column_id, num_row,
                                    num_distinct, num_null, most_common_vals,
                                    most_common_freqs, histogram_bounds);

    catalog::InsertTuple(stats_table, std::move(table_tuple), txn);
  }

  txn_manager.CommitTransaction(txn);
}

/**
 * Generate a column stats tuple.
 * TODO: deal with array type.
 */
std::unique_ptr<storage::Tuple> StatsStorage::GetColumnStatsTuple(
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    oid_t column_id, int num_row, int num_distinct, int num_null,
    UNUSED_ATTRIBUTE std::vector<type::Value> most_common_vals,
    UNUSED_ATTRIBUTE std::vector<int> most_common_freqs,
    UNUSED_ATTRIBUTE std::vector<double> histogram_bounds) {
  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
  auto val1 = type::ValueFactory::GetIntegerValue(database_id);
  auto val2 = type::ValueFactory::GetIntegerValue(table_id);
  auto val3 = type::ValueFactory::GetIntegerValue(column_id);
  auto val4 = type::ValueFactory::GetIntegerValue(num_row);
  auto val5 = type::ValueFactory::GetIntegerValue(num_distinct);
  auto val6 = type::ValueFactory::GetIntegerValue(num_null);
  //auto val7 = type::ValueFactory::GetIntegerValue(most_common_vals);

  tuple->SetValue(0, val1, nullptr);
  tuple->SetValue(1, val2, nullptr);
  tuple->SetValue(2, val3, nullptr);
  tuple->SetValue(3, val4, nullptr);
  tuple->SetValue(4, val5, nullptr);
  tuple->SetValue(5, val6, nullptr);
  //tuple->SetValue(6, val7, nullptr);
  return std::move(tuple);
}

TableStats *StatsStorage::GetTableStatsWithName(
                          UNUSED_ATTRIBUTE const std::string table_name) {
  return nullptr;
}

ColumnStats *StatsStorage::GetColumnStatsByID(UNUSED_ATTRIBUTE oid_t database_id,
                                          UNUSED_ATTRIBUTE oid_t table_id,
                                          UNUSED_ATTRIBUTE oid_t column_id) {
  storage::DataTable *stats_table = GetStatsTable();
  (void) stats_table;
  return nullptr;
}

void StatsStorage::CollectStatsForAllTables() {
  auto catalog = catalog::Catalog::GetInstance();

  oid_t database_count = catalog->GetDatabaseCount();
  for(oid_t db_offset = 0; db_offset < database_count; db_offset ++) {
    auto database = catalog::Catalog::GetInstance()->GetDatabaseWithOffset(db_offset);
    oid_t table_count = database->GetTableCount();
    for (oid_t table_offset = 0; table_offset < table_count; table_offset ++) {
      auto table = database->GetTable(table_offset);
      std::unique_ptr<TableStats> table_stats(new TableStats(table));
      table_stats->CollectColumnStats();
      StoreTableStats(table_stats.get());
    }
  }
}

void StatsStorage::CreateSamplesDatabase() {
  catalog::Catalog::GetInstance()->CreateDatabase(SAMPLES_DB_NAME, nullptr);
}

void StatsStorage::AddSamplesDatatable(
                storage::DataTable *data_table,
                std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples) {
  auto schema = data_table->GetSchema();
  auto schema_copy = catalog::Schema::CopySchema(schema);

  auto catalog = catalog::Catalog::GetInstance();
  auto samples_db = catalog->GetDatabaseWithName(SAMPLES_DB_NAME);
  auto samples_db_oid = samples_db->GetOid();
  bool own_schema = true;
  bool adapt_table = false;
  bool is_catalog = true;

  std::string samples_table_name = GenerateSamplesTableName(data_table->GetDatabaseOid(),
                                                            data_table->GetOid());


  storage::DataTable *table =
        storage::TableFactory::GetDataTable(samples_db_oid, catalog->GetNextOid(),
        schema_copy, samples_table_name, DEFAULT_TUPLES_PER_TILEGROUP,
        own_schema, adapt_table, is_catalog);
  samples_db->AddTable(table, true);

  for (auto &tuple : sampled_tuples) {
    catalog::InsertTuple(table, std::move(tuple), nullptr);
  }
}

} /* namespace optimizer */
} /* namespace peloton */
