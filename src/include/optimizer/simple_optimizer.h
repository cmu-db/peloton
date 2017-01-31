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
#include "parser/statements.h"
#include "type/types.h"
#include "type/value.h"

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

  std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatementList> &parse_tree) override;

 private:
  //===--------------------------------------------------------------------===//
  // UTILITIES
  //===--------------------------------------------------------------------===//

  // get the column IDs evaluated in a predicate
  static void GetPredicateColumns(const catalog::Schema *schema,
                                  expression::AbstractExpression *expression,
                                  std::vector<oid_t> &column_ids,
                                  std::vector<ExpressionType> &expr_types,
                                  std::vector<type::Value> &values,
                                  bool &index_searchable);

  static bool CheckIndexSearchable(storage::DataTable *target_table,
                                   expression::AbstractExpression *expression,
                                   std::vector<oid_t> &key_column_ids,
                                   std::vector<ExpressionType> &expr_types,
                                   std::vector<type::Value> &values,
                                   oid_t &index_id);

  // create a scan plan for a select statement
  static std::unique_ptr<planner::AbstractScan> CreateScanPlan(
      storage::DataTable *target_table, std::vector<oid_t> &column_ids,
      expression::AbstractExpression *predicate, bool for_update);

  // create a copy plan for a copy statement
  static std::unique_ptr<planner::AbstractPlan> CreateCopyPlan(
      parser::CopyStatement *copy_stmt);

  static std::unique_ptr<planner::AbstractPlan> CreateHackingNestedLoopJoinPlan(
      const parser::SelectStatement *statement);
  static std::unique_ptr<planner::AbstractPlan> CreateJoinPlan(
      parser::SelectStatement *select_stmt);

  // This is used for order_by + limit optimization. Let the index scan executor
  // know order_by flags when create an order_by
  // plan. This is used when we create a order_by plan and the underlying
  // plan is index scan. Then we pass these flags to index and index can
  // output limit number tuples. But now, it only works when limit is 1
  static void SetIndexScanFlag(planner::AbstractPlan *select_plan,
                               uint64_t limit, uint64_t offset,
                               bool descent = false);

  // This is used for order_by optimization. When creating a order_by plan,
  // it checks whether the underlying plan has the same output order with
  // order_by plan. Same means: 1) for underlying index scan, its all expression
  // types are equal, otherwise, it can't guarantee the output has the same
  // ordering with order_by expression. 2) the underlying output has the same
  // ascending or descending with order_by plan. 3) order_by column is within
  // the key column ids (lookup ids) with the underlying plan or order_by column
  // plus key column ids are the prefix of the index
  static bool UnderlyingSameOrder(planner::AbstractPlan *select_plan,
                                  oid_t orderby_column_id,
                                  bool order_by_descending);

  std::unique_ptr<planner::AbstractPlan> CreateOrderByLimitPlan(
      parser::SelectStatement *select_stmt, planner::AbstractPlan *child_plan,
      catalog::Schema *schema, std::vector<oid_t> column_ids, bool is_star);

  std::unique_ptr<planner::AbstractPlan> CreateOrderByPlan(
      parser::SelectStatement *select_stmt, planner::AbstractPlan *child_plan,
      catalog::Schema *schema, std::vector<oid_t> column_ids, bool is_star);
};
}  // namespace optimizer
}  // namespace peloton
