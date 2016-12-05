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
#include "common/value.h"

#include <memory>
#include <vector>

namespace peloton {
namespace parser {
class SQLStatement;
class SQLStatementList;
class SelectStatement;
class CopyStatement;
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

  static std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatementList> &parse_tree);

 private:
  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // get the column IDs evaluated in a predicate
  static void GetPredicateColumns(catalog::Schema *schema,
                                  expression::AbstractExpression *expression,
                                  std::vector<oid_t> &column_ids,
                                  std::vector<ExpressionType> &expr_types,
                                  std::vector<common::Value> &values,
                                  bool &index_searchable);

  static bool CheckIndexSearchable(storage::DataTable *target_table,
                                   expression::AbstractExpression *expression,
                                   std::vector<oid_t> &key_column_ids,
                                   std::vector<ExpressionType> &expr_types,
                                   std::vector<common::Value> &values,
                                   oid_t &index_id);

  // create a scan plan for a select statement
  static std::unique_ptr<planner::AbstractScan> CreateScanPlan(
      storage::DataTable *target_table, std::vector<oid_t> &column_ids,
      expression::AbstractExpression *predicate, bool for_update);

  // create a copy plan for a copy statement
  static std::unique_ptr<planner::AbstractPlan> CreateCopyPlan(
      parser::CopyStatement *copy_stmt);

  static std::unique_ptr<planner::AbstractPlan> CreateHackingJoinPlan();
  static std::unique_ptr<planner::AbstractPlan> CreateHackingNestedLoopJoinPlan(
      const parser::SelectStatement *statement);
};
}  // namespace optimizer
}  // namespace peloton
