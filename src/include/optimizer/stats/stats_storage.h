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

#include "optimizer/stats/table_stats_collector.h"
#include "optimizer/stats/column_stats_collector.h"

#include <sstream>

#include "common/macros.h"
#include "common/internal_types.h"
#include "type/value_factory.h"

namespace peloton {

namespace storage {
class DataTable;
}

namespace optimizer {

using ValueFrequencyPair = std::pair<type::Value, double>;

class ColumnStats;
class TableStats;

class StatsStorage {
 public:
  // Global Singleton
  static StatsStorage *GetInstance();

  StatsStorage();

  /* Functions for managing stats table and schema */

  void CreateStatsTableInCatalog();

  /* Functions for adding, updating and quering stats */

  void InsertOrUpdateTableStats(storage::DataTable *table,
                                TableStatsCollector *table_stats_collector,
                                concurrency::TransactionContext *txn = nullptr);

  void InsertOrUpdateColumnStats(
      oid_t database_id, oid_t table_id, oid_t column_id, int num_rows,
      double cardinality, double frac_null, std::string most_common_vals,
      std::string most_common_freqs, std::string histogram_bounds,
      std::string column_name, bool has_index = false,
      concurrency::TransactionContext *txn = nullptr);

  std::shared_ptr<ColumnStats> GetColumnStatsByID(oid_t database_id,
                                                  oid_t table_id,
                                                  oid_t column_id);

  std::shared_ptr<TableStats> GetTableStats(oid_t database_id, oid_t table_id);

  std::shared_ptr<TableStats> GetTableStats(oid_t database_id, oid_t table_id,
                                            std::vector<oid_t> column_ids);

  /* Functions for triggerring stats collection */

  ResultType AnalyzeStatsForAllTables(concurrency::TransactionContext *txn = nullptr);

  ResultType AnalyzeStatsForTable(storage::DataTable *table,
                                  concurrency::TransactionContext *txn = nullptr);

  ResultType AnalayzeStatsForColumns(storage::DataTable *table,
                                     std::vector<std::string> column_names);

 private:
  std::unique_ptr<type::AbstractPool> pool_;

  std::shared_ptr<ColumnStats> ConvertVectorToColumnStats(
      oid_t database_id, oid_t table_id, oid_t column_id,
      std::unique_ptr<std::vector<type::Value>> &column_stats_vector);

  std::string ConvertDoubleArrayToString(std::vector<double> &double_array) {
    if (double_array.size() == 0) {
      return std::string();
    }
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

  std::pair<std::string, std::string> ConvertValueFreqArrayToStrings(
      std::vector<ValueFrequencyPair> &val_freqs) {
    size_t array_size = val_freqs.size();
    if (array_size == 0) {
      return std::make_pair("", "");
    }
    auto type_id = val_freqs[0].first.GetTypeId();
    if (type_id == type::TypeId::VARBINARY ||
        type_id == type::TypeId::VARCHAR) {
      return std::make_pair("", "");
    }
    std::stringstream ss_value;
    std::stringstream ss_freq;
    for (size_t i = 0; i < array_size; ++i) {
      if (i != 0) {
        ss_value << ",";
        ss_freq << ",";
      }
      ss_value << val_freqs[i].first.ToString();
      ss_freq << val_freqs[i].second;
    }
    return std::make_pair(ss_value.str(), ss_freq.str());
  }
};
}
}
