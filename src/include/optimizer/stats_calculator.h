//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_and_stats_calculator.h
//
// Identification: src/include/optimizer/stats_calculator.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "optimizer/operator_visitor.h"

namespace peloton {
namespace optimizer {

class Memo;
class TableStats;

/**
 * @brief Derive stats for the root group using a group expression's children's
 * stats
 */
class StatsCalculator : public OperatorVisitor {
 public:
  void CalculateStats(GroupExpression *gexpr, ExprSet required_cols,
                      Memo *memo, concurrency::TransactionContext* txn);

  void Visit(const LogicalGet *) override;
  void Visit(const LogicalQueryDerivedGet *) override;
  void Visit(const LogicalInnerJoin *) override;
  void Visit(const LogicalLeftJoin *) override;
  void Visit(const LogicalRightJoin *) override;
  void Visit(const LogicalOuterJoin *) override;
  void Visit(const LogicalSemiJoin *) override;
  void Visit(const LogicalAggregateAndGroupBy *) override;
  void Visit(const LogicalLimit *) override;
  void Visit(const LogicalDistinct *) override;

 private:
  /**
   * @brief Add the base table stats if the base table maintain stats, or else
   * use default stats
   *
   * @param col The column we want to get stats
   * @param table_stats Base table stats
   * @param stats The stats map to add
   * @param copy Specify if we want to make a copy
   */
  void AddBaseTableStats(
      expression::AbstractExpression *col,
      std::shared_ptr<TableStats> table_stats,
      std::unordered_map<std::string, std::shared_ptr<ColumnStats>> &stats,
      bool copy);
  /**
   * @brief Update selectivity for predicate evaluation
   *
   * @param num_rows Number of rows of base table
   * @param predicate_stats The stats for columns in the expression
   * @param predicates conjunction predicates
   */
  void UpdateStatsForFilter(
      size_t num_rows,
      std::unordered_map<std::string, std::shared_ptr<ColumnStats>>
          &predicate_stats,
      const std::vector<AnnotatedExpression> &predicates);

  double CalculateSelectivityForPredicate(
      const std::shared_ptr<TableStats> predicate_table_stats,
      const expression::AbstractExpression *expr);

  GroupExpression *gexpr_;
  ExprSet required_cols_;
  Memo *memo_;
  concurrency::TransactionContext* txn_;
};

}  // namespace optimizer
}  // namespace peloton
