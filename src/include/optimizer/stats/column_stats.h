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


namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ColumnStats
//===--------------------------------------------------------------------===//
class ColumnStats {
 public:
  ColumnStats(size_t num_row, double cardinality, double frac_null,
                 std::vector<double> most_common_vals,
                 std::vector<double> most_common_freqs,
                 std::vector<double> histogram_bounds)
      : num_row(num_row),
        cardinality(cardinality),
        frac_null(frac_null),
        most_common_vals(most_common_vals),
        most_common_freqs(most_common_freqs),
        histogram_bounds(histogram_bounds),
        is_basetable{true} {}

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
