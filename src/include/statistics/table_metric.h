//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric.h
//
// Identification: src/statistics/table_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>
#include <string>
#include <boost/functional/hash.hpp>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "statistics/memory_metric.h"
#include "util/string_util.h"

namespace peloton {
namespace stats {
class TableMetricRawData : public AbstractRawData {
 public:
  inline void IncrementTableReads(std::pair<oid_t, oid_t> db_table_id,
                                  size_t num_read) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][READ] += num_read;
  }

  inline void IncrementTableUpdates(std::pair<oid_t, oid_t> db_table_id) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][UPDATE]++;
  }

  inline void IncrementTableInserts(std::pair<oid_t, oid_t> db_table_id) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][INSERT]++;
  }

  inline void IncrementTableDeletes(std::pair<oid_t, oid_t> db_table_id) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][DELETE]++;
  }

  inline void IncrementTableMemAlloc(std::pair<oid_t, oid_t> db_table_id,
                                     int64_t bytes) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][DELETE] += bytes;
  }

  inline void DecrementTableMemAlloc(std::pair<oid_t, oid_t> db_table_id,
                                     int64_t bytes) {
    auto entry = counters_.find(db_table_id);
    if (entry == counters_.end())
      counters_[db_table_id] = std::vector<int64_t>(NUM_COUNTERS);
    counters_[db_table_id][DELETE] -= bytes;
  }

  void Aggregate(AbstractRawData &other) override;

  // TODO(justin) -- actually implement
  void WriteToCatalog() override {}

  const std::string GetInfo() const override { return "index metric"; }

 private:
  struct pair_hash {
    template <class T1, class T2>
    std::size_t operator()(const std::pair<T1, T2> &p) const {
      size_t seed = 0;
      boost::hash_combine(seed, p.first);
      boost::hash_combine(seed, p.second);

      return seed;
    }
  };

  std::unordered_map<std::pair<oid_t, oid_t>, std::vector<int64_t>, pair_hash>
      counters_;

  // this serves as an index into each table's counter vector
  enum CounterType {
    READ = 0,
    UPDATE,
    INSERT,
    DELETE,
    MEMORY_ALLOC,
    MEMORY_USAGE
  };

  // should be number of possible CounterType values
  static const size_t NUM_COUNTERS = 6;
};

class TableMetric : public AbstractMetric<TableMetricRawData> {
 public:
  inline void OnRead(std::pair<oid_t, oid_t> db_table_id,
                     size_t num_read) override {
    GetRawData()->IncrementTableReads(db_table_id, num_read);
  }

  inline void OnUpdate(std::pair<oid_t, oid_t> db_table_id) override {
    GetRawData()->IncrementTableUpdates(db_table_id);
  }

  inline void OnInsert(std::pair<oid_t, oid_t> db_table_id) override {
    GetRawData()->IncrementTableInserts(db_table_id);
  }

  inline void OnDelete(std::pair<oid_t, oid_t> db_table_id) override {
    GetRawData()->IncrementTableDeletes(db_table_id);
  }

  inline void OnMemoryAlloc(std::pair<oid_t, oid_t> db_table_id,
                            size_t bytes) override {
    GetRawData()->IncrementTableMemAlloc(db_table_id, bytes);
  }

  inline void OnMemoryFree(std::pair<oid_t, oid_t> db_table_id,
                           size_t bytes) override {
    GetRawData()->DecrementTableMemAlloc(db_table_id, bytes);
  }
};
/**
 * Metric for the access and memory of a table
 */
class TableMetricOld : public AbstractMetricOld {
 public:
  typedef std::string TableKey;

  TableMetricOld(MetricType type, oid_t database_id, oid_t table_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  inline AccessMetric &GetTableAccess() { return table_access_; }

  inline MemoryMetric &GetTableMemory() { return table_memory_; }

  inline std::string GetName() { return table_name_; }

  inline oid_t GetDatabaseId() { return database_id_; }

  inline oid_t GetTableId() { return table_id_; }

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  inline void Reset() {
    table_access_.Reset();
    table_memory_.Reset();
  }

  inline bool operator==(const TableMetricOld &other) {
    return database_id_ == other.database_id_ && table_id_ == other.table_id_ &&
           table_name_ == other.table_name_ &&
           table_access_ == other.table_access_;
  }

  inline bool operator!=(const TableMetricOld &other) {
    return !(*this == other);
  }

  void Aggregate(AbstractMetricOld &source);

  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << "  TABLE " << table_name_ << "(OID=";
    ss << table_id_ << ")" << std::endl;
    ;
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << table_access_.GetInfo() << std::endl;
    ss << table_memory_.GetInfo() << std::endl;
    return ss.str();
  }

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // The database ID of this table
  oid_t database_id_;

  // The ID of this table
  oid_t table_id_;

  // The name of this table
  std::string table_name_;

  // The number of tuple accesses
  AccessMetric table_access_{MetricType::ACCESS};

  // The memory stats of table
  MemoryMetric table_memory_{MetricType::MEMORY};
};

}  // namespace stats
}  // namespace peloton
