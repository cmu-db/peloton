//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_optimizer.h
//
// Identification: src/include/optimizer/abstract_optmizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/abstract_optimizer.h"

#include <memory>

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Simple Optimizer
//===--------------------------------------------------------------------===//

class SimpleOptimizer : AbstractOptimizer {
 public:
  SimpleOptimizer(const SimpleOptimizer &) = delete;
  SimpleOptimizer &operator=(const SimpleOptimizer &) = delete;
  SimpleOptimizer(SimpleOptimizer &&) = delete;
  SimpleOptimizer &operator=(SimpleOptimizer &&) = delete;

  SimpleOptimizer();
  virtual ~SimpleOptimizer();

  static std::shared_ptr<planner::AbstractPlan>
  BuildPlanTree(const std::unique_ptr<parser::AbstractParse>& parse_tree);

};

} // namespace optimizer
} // namespace peloton
