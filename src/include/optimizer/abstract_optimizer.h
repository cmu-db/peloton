//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_optimizer.h
//
// Identification: src/include/optimizer/abstract_optmizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

namespace peloton {
namespace planner {
class AbstractPlan;
}

namespace parser{
class AbstractParse;
}

namespace optimizer {

//===--------------------------------------------------------------------===//
// Abstract Optimizer
//===--------------------------------------------------------------------===//
class AbstractOptimizer {
 public:
  AbstractOptimizer(const AbstractOptimizer &) = delete;
  AbstractOptimizer &operator=(const AbstractOptimizer &) = delete;
  AbstractOptimizer(AbstractOptimizer &&) = delete;
  AbstractOptimizer &operator=(AbstractOptimizer &&) = delete;

  AbstractOptimizer();
  virtual ~AbstractOptimizer();

};

} // namespace optimizer
} // namespace peloton
