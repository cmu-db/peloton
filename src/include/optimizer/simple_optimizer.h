//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// simple_optimizer.h
//
// Identification: src/include/optimizer/simple_optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include "optimizer/abstract_optimizer.h"

#include <memory>

namespace peloton {
namespace parser{
  class SQLStatement;
}
namespace optimizer {

//===--------------------------------------------------------------------===//
// Simple Optimizer
//===--------------------------------------------------------------------===//

class SimpleOptimizer : public AbstractOptimizer {
 public:
  SimpleOptimizer(const SimpleOptimizer &) = delete;
  SimpleOptimizer &operator=(const SimpleOptimizer &) = delete;
  SimpleOptimizer(SimpleOptimizer &&) = delete;
  SimpleOptimizer &operator=(SimpleOptimizer &&) = delete;

  SimpleOptimizer();
  virtual ~SimpleOptimizer();

  static std::shared_ptr<planner::AbstractPlan> BuildPlanTree(
      const std::unique_ptr<parser::AbstractParse>& parse_tree);

  static std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatement>& parse_tree);

};

}  // namespace optimizer
}  // namespace peloton
