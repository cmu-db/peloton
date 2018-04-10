//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_model.h
//
// Identification: src/include/brain/cost_model.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "brain/index_selection_util.h"

namespace peloton {
namespace brain {

class Workload;

//===--------------------------------------------------------------------===//
// CostModel
//===--------------------------------------------------------------------===//

class CostModel {
 public:
  /**
   * @brief Constructor
   */
  CostModel() {}

  double GetCost(IndexConfiguration config, Workload workload);

 private:
  // memo for cost of configuration, query
};

}  // namespace brain
}  // namespace peloton
