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

#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"
#include "util/string_util.h"

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

  inline const std::string GetInfo() const {
    std::stringstream ss;
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << "  TABLE " << table_name_ << "(OID=";
    ss << table_id_ << ")" << std::endl;
    ;
    ss << peloton::GETINFO_SINGLE_LINE << std::endl;
    ss << table_access_.GetInfo();
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
};

}  // namespace stats
}  // namespace peloton
