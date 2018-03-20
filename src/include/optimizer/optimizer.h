//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.h
//
// Identification: src/include/optimizer/optimizer.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "optimizer/abstract_optimizer.h"
#include "optimizer/property_set.h"
#include "optimizer/optimizer_metadata.h"

namespace peloton {

namespace parser {
class SQLStatementList;
class SQLStatement;
}

namespace planner {
class AbstractPlan;
};

namespace optimizer {
class OperatorExpression;
}

namespace concurrency {
class TransactionContext;
}

namespace optimizer {

struct QueryInfo {
  QueryInfo(std::vector<expression::AbstractExpression *> &exprs,
            std::shared_ptr<PropertySet> &props)
      : output_exprs(std::move(exprs)), physical_props(props) {}

  std::vector<expression::AbstractExpression *> output_exprs;
  std::shared_ptr<PropertySet> physical_props;
};

//===--------------------------------------------------------------------===//
// Optimizer
//===--------------------------------------------------------------------===//
class Optimizer : public AbstractOptimizer {
  friend class BindingIterator;
  friend class GroupBindingIterator;

 public:
  Optimizer(const Optimizer &) = delete;
  Optimizer &operator=(const Optimizer &) = delete;
  Optimizer(Optimizer &&) = delete;
  Optimizer &operator=(Optimizer &&) = delete;

  Optimizer();

  std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatementList> &parse_tree,
      const std::string default_database_name,
      concurrency::TransactionContext *txn) override;

  void OptimizeLoop(int root_group_id,
                    std::shared_ptr<PropertySet> required_props);

  void Reset() override;

 private:
  /* HandleDDLStatement - Check and handle DDL statment (currently only support
   *CREATE), set
   * is_ddl_stmt to false if there is no DDL statement.
   *
   * tree: a peloton query tree representing a select query
   * return: the DDL plan if it is a DDL statement
   */
  std::unique_ptr<planner::AbstractPlan> HandleDDLStatement(
      parser::SQLStatement *tree, bool &is_ddl_stmt,
      concurrency::TransactionContext *txn);

  /* TransformQueryTree - create an initial operator tree for the given query
   * to be used in performing optimization.
   *
   * tree: a peloton query tree representing a select query
   * return: the root group expression for the inserted query
   */
  std::shared_ptr<GroupExpression> InsertQueryTree(
      parser::SQLStatement *tree, concurrency::TransactionContext *txn);

  /* GetQueryTreeRequiredProperties - get the required physical properties for
   * a peloton query tree.
   *
   * tree: a peloton query tree representing a select query
   * return: the set of required physical properties for the query
   */
  QueryInfo GetQueryInfo(parser::SQLStatement *tree);

  /* ChooseBestPlan - retrieve the lowest cost tree of physical operators for
   *     the given properties
   *
   * id: the id of the group to produce the best physical
   * requirements: the set of properties the produced physical operator tree
   *     must satisfy
   * output_expr_map: The map of expression generate by this group to their
   * corresponding offsets
   * return: the lowest cost tree of physical plan nodes
   */
  std::unique_ptr<planner::AbstractPlan> ChooseBestPlan(
      GroupID id, std::shared_ptr<PropertySet> required_props,
      std::vector<expression::AbstractExpression *> required_cols);

  //////////////////////////////////////////////////////////////////////////////
  /// Metadata
  OptimizerMetadata metadata_;
};

}  // namespace optimizer
}  // namespace peloton
