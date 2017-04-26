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

#include "optimizer/stats/table_stats.h"
#include "optimizer/stats/column_stats.h"

#include <sstream>

#include "common/macros.h"
#include "type/types.h"
#include "type/value_factory.h"

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

using ValueFrequencyPair = std::pair<type::Value, double>;

class StatsStorage {
 public:
  // Global Singleton
  static StatsStorage *GetInstance();

  StatsStorage();

  ~StatsStorage();

  /* Functions for managing stats table and schema */

  void CreateStatsCatalog();

  /* Functions for adding, updating and quering stats */

  void AddOrUpdateTableStats(storage::DataTable *table,
                             TableStats *table_stats);

  std::unique_ptr<ColumnStats> GetColumnStatsByID(oid_t database_id,
                                                  oid_t table_id,
                                                  oid_t column_id);

  /* Functions for managing tuple samples */

  void CreateSamplesDatabase();

  void AddSamplesTable(
      storage::DataTable *data_table,
      std::vector<std::unique_ptr<storage::Tuple>> &sampled_tuples);

  bool InsertSampleTuple(storage::DataTable *samples_table,
                         std::unique_ptr<storage::Tuple> tuple,
                         concurrency::Transaction *txn);

  void GetTupleSamples(oid_t database_id, oid_t table_id,
                       std::vector<storage::Tuple> &tuple_samples);

  void GetColumnSamples(oid_t database_id, oid_t table_id, oid_t column_id,
                        std::vector<type::Value> &column_samples);

  std::string GenerateSamplesTableName(oid_t db_id, oid_t table_id) {
    return std::to_string(db_id) + "_" + std::to_string(table_id);
  }

  /* Functions for triggerring stats collection */

  void CollectStatsForAllTables();

 private:
  std::unique_ptr<type::AbstractPool> pool_;

  std::unique_ptr<storage::Tuple> GetColumnStatsTuple(
      const catalog::Schema *schema, oid_t database_id, oid_t table_id,
      oid_t column_id, int num_row, double cardinality, double frac_null,
      std::vector<ValueFrequencyPair> &most_common_val_freqs,
      std::vector<double> &histogram_bounds);

  std::string ConvertDoubleArrayToString(std::vector<double> &double_array) {
    std::stringstream ss;
    for (size_t i = 0; i < double_array.size(); ++i) {
      if (i != 0) {
        ss << ",";
      }
      ss << double_array[i];
    }
    return ss.str();
  }

  std::vector<double> ConvertStringToDoubleArray(std::string str) {
    std::vector<double> double_array;
    std::stringstream ss(str);
    double num;

    while (ss >> num) {
      double_array.push_back(num);

      if (ss.peek() == ',') {
        ss.ignore();
      }
    }

    return double_array;
  }
};
}
}
