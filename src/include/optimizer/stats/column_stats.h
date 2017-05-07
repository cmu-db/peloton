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

#include "common/macros.h"
#include "type/types.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStats
//===--------------------------------------------------------------------===//
class ColumnStats {
 public:
  ColumnStats(oid_t database_id, oid_t table_id, oid_t column_id,
              const std::string column_name, size_t num_row, double cardinality,
              double frac_null, std::vector<double> most_common_vals,
              std::vector<double> most_common_freqs,
              std::vector<double> histogram_bounds)
      : database_id(database_id),
        table_id(table_id),
        column_id(column_id),
        column_name(column_name),
        num_row(num_row),
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

  size_t num_row;
  double cardinality;
  double frac_null;
  std::vector<double> most_common_vals;
  std::vector<double> most_common_freqs;
  std::vector<double> histogram_bounds;

  bool is_basetable;
};

} /* namespace optimizer */
} /* namespace peloton */
