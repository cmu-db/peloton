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

expression::AbstractExpression* Rewriter::RewriteExpression(const expression::AbstractExpression *expr) {
  // (TODO): convert AbstractExpression to AbsExpr_Expression...
  // This is needed in order to provide template classes the correct interface.
  // This should probably be better abstracted away.
  std::shared_ptr<GroupExpressionTemplate> gexpr = ConvertTree(expr);
  std::cout << "Converted tree to internal data structures\n";

  GroupID root_id = gexpr->GetGroupID();
  RewriteLoop(root_id);
  std::cout << "Performed rewrite loop pass\n";

  // (TODO): rebuild AbstractExpression tree from memo
  // The real strategy is very similar to Optimizer::ChooseBestPlan
  // It should be possible to use the Children stored in GroupExpression
  // to recursively pull from memo_ until a GroupExpression where
  // GetChildrenGroupsSize() == 0 (which indicates the leaf).
  
  // For now, this just returns the top level node
  GroupTemplate* group = metadata_.memo.GetGroupByID(root_id);
  std::vector<std::shared_ptr<GroupExpressionTemplate>> exprs = group->GetLogicalExpressions();

  PELOTON_ASSERT(exprs.size() > 0);
  std::cout << "Final logical expressions retrieved\n";

  // Take the first one
  gexpr = exprs[0];
  PELOTON_ASSERT(gexpr->GetChildrenGroupsSize() == 0);

  // (TODO): build a layer which can go from AbsExpr_Container -> new AbstractExpression
  // (TODO): build a layer which can go from AbsExpr_Expression -> new AbstractExpression
  // right now this is just hard-coded which is bad
  PELOTON_ASSERT(gexpr->Op().GetType() == ExpressionType::VALUE_CONSTANT);
  auto casted = static_cast<const expression::ConstantValueExpression*>(gexpr->Op().GetExpr());
  auto rebuilt = new expression::ConstantValueExpression(casted->GetValue());
  std::cout << "Rebuilt expression\n";

  Reset();
  std::cout << "Reset the rewriter\n";
  return rebuilt;
}

void Rewriter::Reset() {
  metadata_ = OptimizerMetadataTemplate(nullptr);
}

std::shared_ptr<AbsExpr_Expression> Rewriter::ConvertToAbsExpr(const expression::AbstractExpression* expr) {

  // (TODO): need to think about how memory management would work w.r.t Peloton/terrier
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
  std::cout << "Entered Rewriter::ConvertTree\n";

  std::shared_ptr<AbsExpr_Expression> exp = ConvertToAbsExpr(expr);
  std::cout << "Converted to AbsExpr_Expression\n";

  std::shared_ptr<GroupExpressionTemplate> gexpr;
  metadata_.RecordTransformedExpression(exp, gexpr);
  std::cout << "Initial loaded into memo\n";
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
