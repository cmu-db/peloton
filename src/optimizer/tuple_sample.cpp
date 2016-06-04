//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sample.cpp
//
// Identification: src/optimizer/tuple_sample.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/tuple_sample.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// TupleSample
//===--------------------------------------------------------------------===//
TupleSample::TupleSample(std::vector<Column *> columns,
                         storage::TileGroup *sampled_tuples)
  : columns(columns), sampled_tuples(sampled_tuples)
{
}

} /* namespace optimizer */
} /* namespace peloton */
