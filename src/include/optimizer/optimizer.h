//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// optimizer.h
//
// Identification: src/include/optimizer/optimizer.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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
}  // namespace parser

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace optimizer {
class OperatorExpression;
}  // namespace optimizer

namespace concurrency {
class TransactionContext;
}  // namespace concurrency

namespace test {
class OptimizerRuleTests_SimpleAssociativeRuleTest_Test;
class OptimizerRuleTests_SimpleAssociativeRuleTest2_Test;
}  // namespace test

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

  friend class ::peloton::test::
      OptimizerRuleTests_SimpleAssociativeRuleTest_Test;
  friend class ::peloton::test::
      OptimizerRuleTests_SimpleAssociativeRuleTest2_Test;

 public:
  Optimizer(const Optimizer &) = delete;
  Optimizer &operator=(const Optimizer &) = delete;
  Optimizer(Optimizer &&) = delete;
  Optimizer &operator=(Optimizer &&) = delete;

  Optimizer();

  std::shared_ptr<planner::AbstractPlan> BuildPelotonPlanTree(
      const std::unique_ptr<parser::SQLStatementList> &parse_tree_list,
      concurrency::TransactionContext *txn) override;

  void OptimizeLoop(int root_group_id,
                    std::shared_ptr<PropertySet> required_props);

  void Reset() override;

  OptimizerMetadata &GetMetadata() { return metadata_; }

  /* For test purposes only */
  std::shared_ptr<GroupExpression> TestInsertQueryTree(
      parser::SQLStatement *tree, concurrency::TransactionContext *txn) {
    return InsertQueryTree(tree, txn);
  }
  /* For test purposes only */
  void TestExecuteTaskStack(OptimizerTaskStack &task_stack, int root_group_id,
                            std::shared_ptr<OptimizeContext> root_context) {
    return ExecuteTaskStack(task_stack, root_group_id, root_context);
  }

 private:
  /**
   * Check and handle the provided DDL statement, returning the resulting plan
   * if parsed tree is a DDL statement. The is_ddl_stmt parameter is set to
   * indicate if the parse tree was indeed a DDL statement.
   *
   * @param tree A parsed SQL statement
   * @param[out] is_ddl_stmt Set to true if the SQL statement is DDL
   * @param txn The transactional context
   * @return The constructed plan tree representing the DDL statement
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

  /* GetOperatorInfo - get the best operator tree in the memo
   * 
   * take ChooseBestPlan as a reference                   
   */
  const std::string GetOperatorInfo(
      GroupID id, std::shared_ptr<PropertySet> required_props, int num_indent);

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

  /* ExecuteTaskStack - Execute elements of given optimization task stack
   * and ensure that we do not go beyond the time limit (unless if one plan has
   *    not been generated yet)
   *
   * task_stack: the optimizer's task stack to iterate through
   * root_group_id: the root group id to check if we have generated a plan or
   *not
   * root_context: the OptimizerContext to use that maintains required
   *properties
   */
  void ExecuteTaskStack(OptimizerTaskStack &task_stack, int root_group_id,
                        std::shared_ptr<OptimizeContext> root_context);

  //////////////////////////////////////////////////////////////////////////////
  /// Metadata
  OptimizerMetadata metadata_;
};

}  // namespace optimizer
}  // namespace peloton
