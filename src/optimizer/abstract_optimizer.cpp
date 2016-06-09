
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_optimizer.cpp
//
// Identification: /peloton/src/optimizer/abstract_optimizer.cpp
//
// Copyright (c) 2016, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/abstract_optimizer.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Abstract Optimizer
//===--------------------------------------------------------------------===//
AbstractOptimizer::AbstractOptimizer(){};

AbstractOptimizer::~AbstractOptimizer(){};

AbstractOptimizer &AbstractOptimizer::GetInstance() {
  thread_local static AbstractOptimizer abstract_optimizer;
  return abstract_optimizer;
}

std::shared_ptr<planner::AbstractPlan> AbstractOptimizer::GeneratePlan(
    std::shared_ptr<std::string> plan_tree) {
  std::cout << "Just a placeholder for plan tree" << plan_tree << std::endl;
  return false;
}

bool AbstractOptimizer::ShouldOptimize(std::string parse) {
  LOG_INFO("Just a placeholder for parse string %s", parse);
  return false;
}

std::shared_ptr<std::string> AbstractOptimizer::TransformParseTreeToOptimizerPlanTree(
    std::string parse) {
	LOG_INFO("Just a placeholder for parse string %s", parse);
	return NULL;
}

} // namespace optimizer
} // namespace peloton
