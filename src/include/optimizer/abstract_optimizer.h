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

#include "common/internal_types.h"

namespace peloton {
namespace planner {
class AbstractPlan;
}

namespace parser {
class AbstractParse;
class SQLStatementList;
}

namespace concurrency {
class TransactionContext;
}

namespace catalog {
class Catalog;
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
      const std::unique_ptr<parser::SQLStatementList> &parse_tree, 
      const std::string default_database_name,
      concurrency::TransactionContext *txn) = 0;

  virtual void Reset(){};
};

}  // namespace optimizer
}  // namespace peloton
