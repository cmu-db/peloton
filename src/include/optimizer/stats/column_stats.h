//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats.h
//
// Identification: src/include/optimizer/stats/column_stats.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <sstream>

#include "common/macros.h"
#include "common/internal_types.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStats
//===--------------------------------------------------------------------===//
class ColumnStats {
 public:
  ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
              const std::string column_name, bool has_index, size_t num_rows,
              double cardinality, double frac_null,
              std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs,
              std::vector<double> histogram_bounds)
      : database_id(database_id),
        table_id(table_id),
        column_id(column_id),
        column_name(column_name),
        has_index(has_index),
        num_rows(num_rows),
        cardinality(cardinality),
        frac_null(frac_null),
        most_common_vals(most_common_vals),
        most_common_freqs(most_common_freqs),
        histogram_bounds(histogram_bounds),
        is_basetable{true} {}

  oid_t database_id;
  oid_t table_id;
  oid_t column_id;
  std::string column_name;
  bool has_index;

  size_t num_rows;
  double cardinality;
  double frac_null;
  std::vector<double> most_common_vals;
  std::vector<double> most_common_freqs;
  std::vector<double> histogram_bounds;

  bool is_basetable;

  std::string ToString() {
    std::ostringstream os;
    os << "column_id :" << column_id << "\n"
       << "column_name :" << column_name << "\n"
       << "num_rows :" << num_rows << "\n";
    return os.str();
  }

  // vector of double to comma seperated string
  std::string VectorToString(const std::vector<double>& vec) {
    std::ostringstream os;
    for (auto v : vec) {
      os << v << ", ";
    }
    std::string res = os.str();
    if (res.size() > 0) {
      res.pop_back();
    }
    return res;
  }

  std::string ToCSV() {
    std::ostringstream os;
    os << column_id << "|" << column_name << "|" << num_rows << "|" << has_index
       << "|" << cardinality << "|" << frac_null << "|"
       << VectorToString(most_common_vals) << "|"
       << VectorToString(most_common_freqs) << "|"
       << VectorToString(histogram_bounds) << "\n";
    return os.str();
  }
};

}  // namespace optimizer
}  // namespace peloton
