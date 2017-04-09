//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_storage.h
//
// Identification: src/include/optimizer/stats/stats_storage.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

// #include "optimizer/stats/table_stats.h"
// #include "optimizer/stats/column_stats.h"

#include "common/macros.h"
#include "type/types.h"

namespace peloton {

namespace storage {
  class DataTable;
  class Tuple;
}

namespace catalog {
  class Schema;
}

namespace optimizer {

#define STATS_TABLE_NAME "stats"

#define SAMPLES_DB_NAME "samples_db"


class ColumnStats {
 public:

};

class TableStats {
 public:
  TableStats(UNUSED_ATTRIBUTE storage::DataTable *table) {}

  void CollectColumnStats() {}
  oid_t GetDatabaseID() { return 0; }
  oid_t GetTableID() { return 0; }
  int GetRowCount() { return 0; }
  oid_t GetColumnCount() { return 0; }
  ColumnStats *GetColumnStats(UNUSED_ATTRIBUTE oid_t column_id) { return nullptr; }
  int GetDistinctCount() { return 0; }
  int GetNullCount() { return 0; }

};


class StatsStorage {
 public:
  // Global Singleton
  static StatsStorage *GetInstance();

  StatsStorage();

  /* Functions for managing stats table and schema */

  void CreateStatsCatalog();

  std::unique_ptr<catalog::Schema> InitializeStatsSchema();

  storage::DataTable *GetStatsTable();

  /* Functions for adding, updating and quering stats */

  void StoreTableStats(TableStats *table_stats);

  void InsertColumnStats();

  std::unique_ptr<storage::Tuple> GetColumnStatsTuple(
    const catalog::Schema *schema, oid_t database_id, oid_t table_id,
    oid_t column_id, int num_row, int num_distinct, int num_null,
    std::vector<type::Value> most_common_vals, std::vector<int> most_common_freqs,
    std::vector<double> histogram_bounds);

  TableStats *GetTableStatsWithName(const std::string table_name);

  ColumnStats *GetColumnStatsByID(UNUSED_ATTRIBUTE oid_t database_id,
                                  UNUSED_ATTRIBUTE oid_t table_id,
                                  UNUSED_ATTRIBUTE oid_t column_id);

  /* Functions for managing tuple samples */

  void CreateSamplesDatabase();

  void AddSamplesDatatable(
                storage::DataTable *data_table,
                std::vector<storage::Tuple> sampled_tuples);


  /* Functions for triggerring stats collection */

  void CollectStatsForAllTables();

 private:
  void StoreColumnStats(ColumnStats *column_stats);


};

}

}
