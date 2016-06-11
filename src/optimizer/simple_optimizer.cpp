
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// simple_optimizer.cpp
//
// Identification: /peloton/src/optimizer/simple_optimizer.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/simple_optimizer.h"

namespace peloton {
namespace optimizer {

SimpleOptimizer::SimpleOptimizer(){};

SimpleOptimizer::~SimpleOptimizer(){};

std::shared_ptr<planner::AbstractPlan>
SimpleOptimizer::BuildPlanTree(const std::unique_ptr<parser::AbstractParse>&) {
  std::shared_ptr<planner::AbstractPlan> plan_tree;

  // TODO: Transform the parse tree

  return plan_tree;
}


} // namespace optimizer
} // namespace peloton
