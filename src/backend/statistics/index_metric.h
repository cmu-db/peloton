//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_metric.h
//
// Identification: src/backend/statistics/index_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>
#include <iostream>
#include <string>
#include <sstream>


#include "backend/common/types.h"
#include "backend/statistics/counter_metric.h"
#include "backend/statistics/abstract_metric.h"
#include "backend/statistics/access_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Metric of index accesses
 */
class IndexMetric : public AbstractMetric {
 public:

  typedef std::string IndexKey;

  static inline IndexKey GetKey(oid_t database_id, oid_t table_id, oid_t index_id) {
    std::stringstream ss;
    ss << database_id << table_id << index_id;
    return ss.str();
  }

  IndexMetric(MetricType type, oid_t database_id, oid_t table_id, oid_t index_id);

  inline AccessMetric& GetIndexAccess() {
    return index_access_;
  }

  inline std::string GetName() {
    return index_name_;
  }

  inline oid_t GetDatabaseId() {
    return database_id_;
  }

  inline oid_t GetTableId() {
    return table_id_;
  }

  inline oid_t GetIndexId() {
    return index_id_;
  }

  inline void Reset() {
    index_access_.Reset();
  }

  inline bool operator==(const IndexMetric &other) {
    return database_id_  == other.database_id_ &&
           table_id_     == other.table_id_    &&
           index_id_     == other.index_id_    &&
           index_name_   == other.index_name_  &&
           index_access_ == other.index_access_;
  }

  inline bool operator!=(const IndexMetric &other) {
    return !(*this == other);
  }

  void Aggregate(AbstractMetric &source);

  inline std::string ToString() {
    std::stringstream ss;
    ss << "INDEXES: " << std::endl;
    ss << index_name_ << "(OID=" << index_id_ << "): ";
    ss << index_access_.ToString() << std::endl;
    return ss.str();
  }

 private:
  oid_t database_id_;
  oid_t table_id_;
  oid_t index_id_;
  std::string index_name_;
  AccessMetric index_access_{ACCESS_METRIC};

};

}  // namespace stats
}  // namespace peloton
