//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_optimizer.h
//
// Identification: src/include/optimizer/abstract_optimizer.h
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

namespace parser {
class AbstractParse;
class SQLStatementList;
}

namespace concurrency {
class Transaction;
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

  virtual std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::shared_ptr<parser::SQLStatementList> &parse_tree, concurrency::Transaction *txn) = 0;

  virtual void Reset(){};
};

}  // namespace optimizer
}  // namespace peloton
