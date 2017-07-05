//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats.h
//
// Identification: src/include/optimizer/stats.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/tuple_sample.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Stats
//===--------------------------------------------------------------------===//
class Stats {
 public:
  Stats(TupleSample *sample) : sample_(sample){};

 private:
   TupleSample *sample_;
};

} /* namespace optimizer */
} /* namespace peloton */
