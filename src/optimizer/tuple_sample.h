//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_sample.h
//
// Identification: src/backend/optimizer/tuple_sample.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/optimizer/column.h"
#include "backend/storage/tile_group.h"
#include "backend/optimizer/util.h"

#include <vector>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// TupleSample
//===--------------------------------------------------------------------===//
class TupleSample {
 public:
  TupleSample(std::vector<Column *> columns,
              storage::TileGroup *sampled_tuples);

 private:
  std::vector<Column *> columns;
  storage::TileGroup *sampled_tuples;
};

} /* namespace optimizer */
} /* namespace peloton */
