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

#include "type/types.h"

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
      const std::unique_ptr<parser::SQLStatementList> &parse_tree, concurrency::Transaction *txn) = 0;
  
  inline void SetDefaultDatabaseName(std::string default_database_name) {
      this->default_database_name_ = default_database_name;
  }

  virtual void Reset(){};

  std::string default_database_name_ = DEFAULT_DB_NAME;
};

}  // namespace optimizer
}  // namespace peloton
