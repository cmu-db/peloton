//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_metric.h
//
// Identification: src/backend/statistics/table_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "backend/common/types.h"
#include "backend/statistics/abstract_metric.h"
#include "backend/statistics/access_metric.h"

namespace peloton {
namespace stats {

/**
 * Metric for the access of a table
 */
class TableMetric : public AbstractMetric {
 public:
  typedef std::string TableKey;

  TableMetric(MetricType type, oid_t database_id, oid_t table_id);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // Returns a key for this index
  static inline TableKey GetKey(oid_t database_id, oid_t table_id) {
    std::stringstream ss;
    ss << database_id << table_id;
    return ss.str();
  }

  inline AccessMetric &GetTableAccess() { return table_access_; }

  inline std::string GetName() { return table_name_; }

  inline oid_t GetDatabaseId() { return database_id_; }

  inline oid_t GetTableId() { return table_id_; }

  //===--------------------------------------------------------------------===//
  // HELPER FUNCTIONS
  //===--------------------------------------------------------------------===//

  inline void Reset() { table_access_.Reset(); }

  inline bool operator==(const TableMetric &other) {
    return database_id_ == other.database_id_ && table_id_ == other.table_id_ &&
           table_name_ == other.table_name_ &&
           table_access_ == other.table_access_;
  }

  inline bool operator!=(const TableMetric &other) { return !(*this == other); }

  void Aggregate(AbstractMetric &source);

  inline std::string ToString() const {
    std::stringstream ss;
    ss << "-----------------------------" << std::endl;
    ss << "  TABLE " << table_name_ << "(OID=";
    ss << table_id_ << ")" << std::endl;
    ;
    ss << "-----------------------------" << std::endl;
    ss << table_access_.ToString() << std::endl;
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
  AccessMetric table_access_{ACCESS_METRIC};
};

}  // namespace stats
}  // namespace peloton
