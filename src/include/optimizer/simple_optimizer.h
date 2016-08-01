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
#include "common/types.h"

#include <memory>
#include <vector>

namespace peloton {
namespace parser {
class SQLStatement;
class SelectStatement;
}

namespace storage {
class DataTable;
}

namespace catalog {
class Schema;
}

namespace expression {
class AbstractExpression;
}

namespace planner {
class AbstractScan;
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
      const std::unique_ptr<parser::AbstractParse> &parse_tree);

  static std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatement> &parse_tree);

 private:
  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // get the column IDs evaluated in a predicate
  static void GetPredicateColumns(catalog::Schema *schema,
                                  expression::AbstractExpression *expression,
                                  std::vector<oid_t> &column_ids,
                                  std::vector<ExpressionType> &expr_types,
                                  std::vector<Value> &values,
                                  bool &index_searchable);

  // create a scan plan for a select statement
  static std::unique_ptr<planner::AbstractScan> CreateScanPlan(
      storage::DataTable *target_table, parser::SelectStatement *select_stmt);

  static std::unique_ptr<planner::AbstractScan> CreateHackingJoinPlan(
      parser::SelectStatement *select_stmt);
};
}  // namespace optimizer
}  // namespace peloton
