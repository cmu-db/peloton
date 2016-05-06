//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.h
//
// Identification: src/backend/statistics/counter_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>

#include "backend/common/types.h"
#include "backend/statistics/abstract_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Metric as a counter. E.g. # txns committed
 */
class AccessMetric : public AbstractMetric {
 public:

  AccessMetric(MetricType type);

  inline void IncrementReads() {
    read_counts_++;
  }

  inline void IncrementUpdates() {
    update_counts_++;
  }

  inline void IncrementInserts() {
    insert_counts_++;
  }

  inline void IncrementDeletes() {
    delete_counts_++;
  }

  inline int64_t GetReads() {
    return read_counts_;
  }

  inline int64_t GetUpdates() {
    return update_counts_;
  }

  inline int64_t GetInserts() {
    return insert_counts_;
  }

  inline int64_t GetDeletes() {
    return delete_counts_;
  }

  inline void Reset() {
    read_counts_ = 0;
    update_counts_ = 0;
    insert_counts_ = 0;
    delete_counts_ = 0;
  }


  inline std::string ToString() {
    std::stringstream ss;
    ss << "[ reads=" << read_counts_ << ", updates=" << update_counts_ << ", inserts="
        << insert_counts_ << "deletes=" << delete_counts_ << " ]"<< std::endl;
    return ss.str();
  }

  void Aggregate(AbstractMetric &source);

 private:
  int64_t read_counts_;
  int64_t update_counts_;
  int64_t insert_counts_;
  int64_t delete_counts_;

};

}  // namespace stats
}  // namespace peloton
