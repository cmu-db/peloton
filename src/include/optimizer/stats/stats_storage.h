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
}

namespace optimizer {

using ValueFrequencyPair = std::pair<type::Value, double>;

class StatsStorage {
 public:
  // Global Singleton
  static StatsStorage *GetInstance();

  StatsStorage();

  /* Functions for managing stats table and schema */

  void CreateStatsCatalog();

  /* Functions for adding, updating and quering stats */

  void InsertOrUpdateTableStats(storage::DataTable *table,
                                TableStats *table_stats,
                                concurrency::Transaction *txn = nullptr);

  void InsertOrUpdateColumnStats(oid_t database_id, oid_t table_id,
                                 oid_t column_id, int num_row,
                                 double cardinality, double frac_null,
                                 std::string most_common_vals,
                                 double most_common_freqs,
                                 std::string histogram_bounds,
                                 concurrency::Transaction *txn = nullptr);

  std::unique_ptr<std::vector<type::Value>> GetColumnStatsByID(
      oid_t database_id, oid_t table_id, oid_t column_id);

  /* Functions for triggerring stats collection */

  ResultType AnalyzeStatsForAllTables(concurrency::Transaction *txn = nullptr);

  ResultType AnalyzeStatsForTable(storage::DataTable *table,
                                  concurrency::Transaction *txn = nullptr);

  // void AnalayzeStatsForColumns(std::string database_name, std::string
  // table_name, std::vector<std::string> column_names);

 private:
  std::unique_ptr<type::AbstractPool> pool_;

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
