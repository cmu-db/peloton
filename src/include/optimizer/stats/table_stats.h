//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_stats.h
//
// Identification: src/include/optimizer/stats/table_stats.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once


namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// TableStats
//===--------------------------------------------------------------------===//
class TableStats {
 public:
  TableStats(size_t num_row)
      : num_row(num_row) {}

  size_t num_row;

};

} /* namespace optimizer */
} /* namespace peloton */
