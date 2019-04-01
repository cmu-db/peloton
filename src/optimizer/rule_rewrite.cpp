#include <memory>

#include "catalog/column_catalog.h"
#include "catalog/index_catalog.h"
#include "catalog/table_catalog.h"
#include "optimizer/operators.h"
#include "optimizer/absexpr_expression.h"
#include "optimizer/optimizer_metadata.h"
#include "optimizer/properties.h"
#include "optimizer/rule_rewrite.h"
#include "optimizer/util.h"
#include "type/value_factory.h"

namespace peloton {
namespace optimizer {

ComparatorElimination::ComparatorElimination() {
  type_ = RuleType::COMP_EQUALITY_ELIMINATION;

  match_pattern = std::make_shared<Pattern<ExpressionType>>(ExpressionType::COMPARE_EQUAL);
  auto left = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  auto right = std::make_shared<Pattern<ExpressionType>>(ExpressionType::VALUE_CONSTANT);
  match_pattern->AddChild(left);
  match_pattern->AddChild(right);
}

int ComparatorElimination::Promise(GroupExpression<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *group_expr,
                                   OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const {
  (void)group_expr;
  (void)context;

  //(TODO): is this correct, proceed to structural binding?
  std::cout << "Promise hit\n";
  return 1;
}

bool ComparatorElimination::Check(std::shared_ptr<AbsExpr_Expression> plan,
                                  OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const {
  (void)context;
  (void)plan;

  std::cout << "Check hit\n";

  //(TODO): perform checking more gracefully
  // Technically, if structure matches, rule should always be applied
  PELOTON_ASSERT(plan != nullptr);
  PELOTON_ASSERT(plan->Children().size() == 2);
  PELOTON_ASSERT(plan->Op().GetType() == ExpressionType::COMPARE_EQUAL);

  // Verify the structure of the tree is correct
  auto left = plan->Children()[0];
  auto right = plan->Children()[1];
  PELOTON_ASSERT(left->Children().size() == 0);
  PELOTON_ASSERT(left->Op().GetType() == ExpressionType::VALUE_CONSTANT);
  PELOTON_ASSERT(right->Children().size() == 0);
  PELOTON_ASSERT(right->Op().GetType() == ExpressionType::VALUE_CONSTANT);

  return true;
}

void ComparatorElimination::Transform(std::shared_ptr<AbsExpr_Expression> input,
                                      std::vector<std::shared_ptr<AbsExpr_Expression>> &transformed,
                                      UNUSED_ATTRIBUTE OptimizeContext<AbsExpr_Container,ExpressionType,AbsExpr_Expression> *context) const {
  (void)transformed;
  (void)context;

  // (TODO): create a wrapper for evaluating ConstantValue relations

  // Extract the AbstractExpression through indirection layer
  auto left = input->Children()[0]->Op().GetExpr();
  auto right = input->Children()[1]->Op().GetExpr();
  auto lv = static_cast<const expression::ConstantValueExpression*>(left);
  auto rv = static_cast<const expression::ConstantValueExpression*>(right);
  lv = const_cast<expression::ConstantValueExpression*>(lv);
  rv = const_cast<expression::ConstantValueExpression*>(rv);

  // Get the Value from ConstantValueExpression
  auto lvalue = lv->GetValue();
  auto rvalue = rv->GetValue();

  // Need to check type equality to prevent assertion failure
  // This is only a Peloton issue (terrier checks type for you)
  // (TODO): perform checking through a class/strategy
  bool is_equal = (lvalue.GetTypeId() == rvalue.GetTypeId()) &&
                  (lv->ExactlyEquals(*rv));

  // Create the transformed expression
  type::Value val = type::ValueFactory::GetBooleanValue(is_equal);
  auto eq = new expression::ConstantValueExpression(val);
  auto cnt = AbsExpr_Container(eq);
  auto shared = std::make_shared<AbsExpr_Expression>(cnt);

  // (TODO): figure out how to free these expressions
  // (TODO): Terrier uses shared_ptr but Peloton has this
  // awkward mixture of raw pointers and unique_ptr
  transformed.push_back(shared);
}
}  // namespace optimizer
}  // namespace peloton
