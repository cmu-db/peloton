//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rewriter.cpp
//
// Identification: src/optimizer/rewriter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "optimizer/optimizer.h"
#include "optimizer/rewriter.h"
#include "common/exception.h"

#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_visitor.h"
#include "optimizer/optimize_context.h"
#include "optimizer/optimizer_task_pool.h"
#include "optimizer/rule.h"
#include "optimizer/rule_impls.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/absexpr_expression.h"
#include "expression/abstract_expression.h"
#include "expression/constant_value_expression.h"

using std::vector;
using std::unordered_map;
using std::shared_ptr;
using std::unique_ptr;
using std::move;
using std::pair;
using std::make_shared;

namespace peloton {
namespace optimizer {

using OptimizerMetadataTemplate = OptimizerMetadata<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using OptimizeContextTemplate = OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using OptimizerTaskStackTemplate = OptimizerTaskStack<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using TopDownRewriteTemplate = TopDownRewrite<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using BottomUpRewriteTemplate = BottomUpRewrite<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using GroupExpressionTemplate = GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

using GroupTemplate = Group<AbsExpr_Container,ExpressionType,AbsExpr_Expression>;

Rewriter::Rewriter() : metadata_(nullptr) {
  metadata_ = OptimizerMetadataTemplate(nullptr);
}

void Rewriter::RewriteLoop(int root_group_id) {
  std::shared_ptr<OptimizeContextTemplate> root_context =
      std::make_shared<OptimizeContextTemplate>(&metadata_, nullptr);
  auto task_stack =
      std::unique_ptr<OptimizerTaskStackTemplate>(new OptimizerTaskStackTemplate());
  metadata_.SetTaskPool(task_stack.get());

  // Perform rewrite first
  task_stack->Push(new BottomUpRewriteTemplate(root_group_id, root_context, RewriteRuleSetName::COMPARATOR_ELIMINATION, false));

  ExecuteTaskStack(*task_stack);
}

expression::AbstractExpression* Rewriter::RebuildExpression(int root) {
  auto cur_group = metadata_.memo.GetGroupByID(root);
  auto exprs = cur_group->GetLogicalExpressions();

  // (TODO): what should we do if exprs.size() > 1?
  PELOTON_ASSERT(exprs.size() > 0);
  auto expr = exprs[0];

  std::vector<GroupID> child_groups = expr->GetChildGroupIDs();
  std::vector<expression::AbstractExpression*> child_exprs;
  for (auto group : child_groups) {
    // Build children first
    expression::AbstractExpression *child = RebuildExpression(group);
    PELOTON_ASSERT(child != nullptr);

    child_exprs.push_back(child);
  }

  AbsExpr_Container c = expr->Op();
  return c.Rebuild(child_exprs);
}

expression::AbstractExpression* Rewriter::RewriteExpression(const expression::AbstractExpression *expr) {
  // (TODO): do we need to actually convert to a wrapper?
  // This is needed in order to provide template classes the correct interface.
  // This should probably be better abstracted away.
  std::shared_ptr<GroupExpressionTemplate> gexpr = ConvertTree(expr);
  LOG_DEBUG("Converted tree to internal data structures");

  GroupID root_id = gexpr->GetGroupID();
  RewriteLoop(root_id);
  LOG_DEBUG("Performed rewrite loop pass");

  expression::AbstractExpression *expr_tree = RebuildExpression(root_id);
  LOG_DEBUG("Rebuilt expression tree from memo table");

  Reset();
  LOG_DEBUG("Reset the rewriter");
  return expr_tree;
}

void Rewriter::Reset() {
  metadata_ = OptimizerMetadataTemplate(nullptr);
}

std::shared_ptr<AbsExpr_Expression> Rewriter::ConvertToAbsExpr(const expression::AbstractExpression* expr) {

  // (TODO): fix memory management once we get to terrier
  // for now, this just directly wraps each AbstractExpression in a AbsExpr_Container
  // which is then wrapped in an AbsExpr_Expression to provide the same Operator/OperatorExpression
  // interface that is relied upon by the rest of the code base.

  auto container = AbsExpr_Container(expr);
  auto exp = std::make_shared<AbsExpr_Expression>(container);
  for (size_t i = 0; i < expr->GetChildrenSize(); i++) {
    exp->PushChild(ConvertToAbsExpr(expr->GetChild(i)));
  }
  return exp;
}

std::shared_ptr<GroupExpressionTemplate> Rewriter::ConvertTree(
  const expression::AbstractExpression *expr) {

  std::shared_ptr<AbsExpr_Expression> exp = ConvertToAbsExpr(expr);
  std::shared_ptr<GroupExpressionTemplate> gexpr;
  metadata_.RecordTransformedExpression(exp, gexpr);
  return gexpr;
}

void Rewriter::ExecuteTaskStack(OptimizerTaskStackTemplate &task_stack) {
  // Iterate through the task stack
  while (!task_stack.Empty()) {
    auto task = task_stack.Pop();
    task->execute();
  }
}

}  // namespace optimizer
}  // namespace peloton
