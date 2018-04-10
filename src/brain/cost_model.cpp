//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_model.cpp
//
// Identification: src/brain/cost_model.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/cost_model.h"
#include "brain/index_selection.h"
#include "brain/what_if_index.h"
#include "common/logger.h"
#include "optimizer/optimizer.h"

namespace peloton {
namespace brain {

double CostModel::GetCost(IndexConfiguration config, Workload workload) {
  double cost = 0.0;
  (void)config;
  (void)workload;
  // for (auto query : workload) {
  //   result = WhatIfIndex::GetCostAndPlanTree(query, config, DEFAULT_DB_NAME);

  // }
  return cost;
}

}  // namespace brain
}  // namespace peloton
