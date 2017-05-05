//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost.h
//
// Identification: src/include/optimizer/stats/cost.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "common/macros.h"

#include <cmath>
#include <vector>
#include <cassert>
#include <cstdlib>
#include <cinttypes>

namespace peloton{
namespace optimizer{

class Cost {
public:

  static void SeqScan() {

  }

  static void IndexScan() {

  }

  static void NestedLoopJoin() {

  }

  static void HashJoin() {

  }

  static void SortMergeJoin() {

  }
};
} /* namespace optimizer */
} /* namespace peloton */
