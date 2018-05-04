//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.h
//
// Identification: src/statistics/index_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "util/string_util.h"

namespace peloton {
namespace stats {
class IndexMetricRawData : public AbstractRawData {
 public:
  inline void IncrementIndexReads(std::pair<oid_t, oid_t> db_index_id,
                                  size_t num_read) {
    auto entry = counters_.find(db_index_id);
    if (entry == counters_.end())
      counters_[db_index_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_index_id][READ] += num_read;
  }

  inline void IncrementIndexUpdates(std::pair<oid_t, oid_t> db_index_id) {
    auto entry = counters_.find(db_index_id);
    if (entry == counters_.end())
      counters_[db_index_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_index_id][UPDATE]++;
  }

  inline void IncrementIndexInserts(std::pair<oid_t, oid_t> db_index_id) {
    auto entry = counters_.find(db_index_id);
    if (entry == counters_.end())
      counters_[db_index_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_index_id][INSERT]++;
  }

  inline void IncrementIndexDeletes(std::pair<oid_t, oid_t> db_index_id) {
    auto entry = counters_.find(db_index_id);
    if (entry == counters_.end())
      counters_[db_index_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_index_id][DELETE]++;
  }

  void Aggregate(AbstractRawData &other) override;

  void WriteToCatalog() override;

  const std::string GetInfo() const override { return "index metric"; }

 private:
  std::unordered_map<std::pair<oid_t, oid_t>, std::vector<int64_t>, pair_hash>
      counters_;

  // this serves as an index into each table's counter vector
  enum CounterType { READ = 0, UPDATE, INSERT, DELETE };

  // should be number of possible CounterType values
  static const size_t NUM_COUNTERS = 4;
};

class IndexMetric : public AbstractMetric<IndexMetricRawData> {
 public:
  inline void OnIndexRead(std::pair<oid_t, oid_t> db_index_id,
                          size_t num_read) override {
    GetRawData()->IncrementIndexReads(db_index_id, num_read);
  }

  inline void OnIndexUpdate(std::pair<oid_t, oid_t> db_index_id) override {
    GetRawData()->IncrementIndexUpdates(db_index_id);
  }

  inline void OnIndexInsert(std::pair<oid_t, oid_t> db_index_id) override {
    GetRawData()->IncrementIndexInserts(db_index_id);
  }

  inline void OnIndexDelete(std::pair<oid_t, oid_t> db_index_id) override {
    GetRawData()->IncrementIndexDeletes(db_index_id);
  }
};
/**
 * Metric of index accesses and other index-specific metrics.
 */
class IndexMetricOld : public AbstractMetricOld {
 public:
  typedef std::string IndexKey;

  IndexMetricOld(MetricType type, oid_t database_id, oid_t table_id,
                 oid_t index_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Returns a metric containing the counts of all
  // accesses to this index
  inline AccessMetric &GetIndexAccess() { return index_access_; }

  inline std::string GetName() { return index_name_; }

  inline oid_t GetDatabaseId() { return database_id_; }

  inline oid_t GetTableId() { return table_id_; }

  inline oid_t GetIndexId() { return index_id_; }

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline void Reset() { index_access_.Reset(); }

  inline bool operator==(const IndexMetricOld &other) {
    return database_id_ == other.database_id_ && table_id_ == other.table_id_ &&
           index_id_ == other.index_id_ && index_name_ == other.index_name_ &&
           index_access_ == other.index_access_;
  }

  inline bool operator!=(const IndexMetricOld &other) {
    return !(*this == other);
  }

  void Aggregate(AbstractMetricOld &source);

  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << "INDEXES: " << std::endl;
    ss << index_name_ << "(OID=" << index_id_ << "): ";
    ss << index_access_.GetInfo();
    return ss.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The database ID of this index
  oid_t database_id_;

  // The table ID of this index
  oid_t table_id_;

  // The ID of this index
  oid_t index_id_;

  // The name of this index
  std::string index_name_;

  // Counts the number of index entries accessed
  AccessMetric index_access_{MetricType::ACCESS};
};

}  // namespace stats
}  // namespace peloton
