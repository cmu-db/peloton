//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.h
//
// Identification: src/include/statistics/index_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "util/string_util.h"

namespace peloton {
namespace stats {
class IndexMetricRawData : public AbstractRawData {
  // this serves as an index into each table's counter vector
  enum CounterType {
    READ = 0,
    UPDATE,
    INSERT,
    DELETE,
    MEMORY_ALLOC,
    MEMORY_USAGE
  };

 public:
  inline void IncrementIndexReads(std::pair<oid_t, oid_t> db_index_id,
                                  size_t num_read) {
    GetCounter(db_index_id, READ) += num_read;
  }

  inline void IncrementIndexUpdates(std::pair<oid_t, oid_t> db_index_id) {
    GetCounter(db_index_id, UPDATE)++;
  }

  inline void IncrementIndexInserts(std::pair<oid_t, oid_t> db_index_id) {
    GetCounter(db_index_id, INSERT)++;
  }

  inline void IncrementIndexDeletes(std::pair<oid_t, oid_t> db_index_id) {
    GetCounter(db_index_id, DELETE)++;
  }

  inline void IncrementIndexMemoryAlloc(std::pair<oid_t, oid_t> db_index_id,
                                        size_t bytes) {
    GetCounter(db_index_id, MEMORY_ALLOC) += bytes;
  }

  inline void DecrementIndexMemoryAlloc(std::pair<oid_t, oid_t> db_index_id,
                                        size_t bytes) {
    GetCounter(db_index_id, MEMORY_ALLOC) -= bytes;
  }

  inline void IncrementIndexMemoryUsage(std::pair<oid_t, oid_t> db_index_id,
                                        size_t bytes) {
    GetCounter(db_index_id, MEMORY_USAGE) += bytes;
  }

  inline void DecrementIndexMemoryUsage(std::pair<oid_t, oid_t> db_index_id,
                                        size_t bytes) {
    GetCounter(db_index_id, MEMORY_USAGE) -= bytes;
  }

  void Aggregate(AbstractRawData &other) override;

  void WriteToCatalog() override;

  const std::string GetInfo() const override { return "index metric"; }

 private:
  inline int64_t &GetCounter(std::pair<oid_t, oid_t> db_index_id,
                              CounterType type) {
    auto entry = counters_.find(db_index_id);
    if (entry == counters_.end())
      counters_[db_index_id] = std::vector<int64_t>(NUM_COUNTERS);
    return counters_[db_index_id][type];
  }

  // map from (database_oid, index_oid) pair to vector of counters,
  // where CounterType enum describes what kind of counter is kept at what index
  std::unordered_map<std::pair<oid_t, oid_t>, std::vector<int64_t>, pair_hash>
      counters_;

  // should be number of possible CounterType values
  static const size_t NUM_COUNTERS = 6;
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
  inline void OnMemoryAlloc(std::pair<oid_t, oid_t> db_index_id, size_t bytes) {
    GetRawData()->IncrementIndexMemoryAlloc(db_index_id, bytes);
  };
  inline void OnMemoryFree(std::pair<oid_t, oid_t> db_index_id, size_t bytes) {
    GetRawData()->DecrementIndexMemoryAlloc(db_index_id, bytes);
  };
  inline void OnMemoryUsage(std::pair<oid_t, oid_t> db_index_id, size_t bytes) {
    GetRawData()->IncrementIndexMemoryUsage(db_index_id, bytes);
  };
  inline void OnMemoryReclaim(std::pair<oid_t, oid_t> db_index_id,
                              size_t bytes) {
    GetRawData()->DecrementIndexMemoryUsage(db_index_id, bytes);
  };
};

}  // namespace stats
}  // namespace peloton
