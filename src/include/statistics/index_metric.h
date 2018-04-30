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
  inline void IncrementIndexReads(oid_t index_id, size_t num_read) {
    auto entry = counters_.find(index_id);
    if(entry != counters_.end()) counters_[index_id] = std::vector<uint64_t>(NUM_COUNTERS);
    counters_[index_id][READ] += num_read;
  }

  inline void IncrementIndexUpdates(oid_t index_id) {
    auto entry = counters_.find(index_id);
    if(entry != counters_.end()) counters_[index_id] = std::vector<uint64_t>(NUM_COUNTERS);
    counters_[index_id][UPDATE]++;
  }

  inline void IncrementIndexInserts(oid_t index_id) {
    auto entry = counters_.find(index_id);
    if(entry != counters_.end()) counters_[index_id] = std::vector<uint64_t>(NUM_COUNTERS);
    counters_[index_id][INSERT]++;
  }

  inline void IncrementIndexDeletes(oid_t index_id) {
    auto entry = counters_.find(index_id);
    if(entry != counters_.end()) counters_[index_id] = std::vector<uint64_t>(NUM_COUNTERS);
    counters_[index_id][DELETE]++;
  }

  void Aggregate(AbstractRawData &other) override {
    auto &other_index_metric = dynamic_cast<IndexMetricRawData &>(other);
    for (auto &entry: other_index_metric.counters_) {
      auto &this_counter = counters_[entry.first];
      auto &other_counter = entry.second;
      for(size_t i = 0; i < NUM_COUNTERS; i++) {
        this_counter[i] += other_counter[i];
      }
    }
  }

  // TODO(justin) -- actually implement
  void WriteToCatalog() override{}

  const std::string GetInfo() const override {
    return "index metric";
  }
private:
  std::unordered_map<oid_t, std::vector<uint64_t>> counters_;

  static const size_t NUM_COUNTERS = 4;

  enum AccessType {
    READ = 0,
    UPDATE,
    INSERT,
    DELETE
  };
};

class IndexMetric: public AbstractMetric<IndexMetricRawData> {
 public:
  inline void OnIndexRead(oid_t index_id, size_t num_read) override {
    GetRawData()->IncrementIndexReads(index_id, num_read);
  }

  inline void OnIndexUpdate(oid_t index_id) override {
    GetRawData()->IncrementIndexUpdates(index_id);
  }

  inline void OnIndexInsert(oid_t index_id) override {
    GetRawData()->IncrementIndexInserts(index_id);
  }

  inline void OnIndexDelete(oid_t index_id) override {
    GetRawData()->IncrementIndexDeletes(index_id);
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

  inline bool operator!=(const IndexMetricOld &other) { return !(*this == other); }

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
