//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.cpp
//
// Identification: src/planner/abstract_plan.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/abstract_plan.h"

#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

#include "common/types.h"
#include "common/logger.h"
#include "catalog/bootstrapper.h"
#include "expression/expression_util.h"

namespace peloton {
namespace planner {

AbstractPlan::AbstractPlan() {}

AbstractPlan::~AbstractPlan() {}

void AbstractPlan::AddChild(std::unique_ptr<AbstractPlan> &&child) {
  children_.emplace_back(std::move(child));
}

const std::vector<std::unique_ptr<AbstractPlan>> &AbstractPlan::GetChildren()
    const {
  return children_;
}

const AbstractPlan *AbstractPlan::GetParent() { return parent_; }

// Get a string representation of this plan
std::ostream &operator<<(std::ostream &os, const AbstractPlan &plan) {
  os << PlanNodeTypeToString(plan.GetPlanNodeType());

  return os;
}

const std::string AbstractPlan::GetInfo() const {
  std::ostringstream os;

  os << GetInfo();

  // Traverse the tree
  std::string child_spacer = "  ";
  for (int ctr = 0, cnt = static_cast<int>(children_.size()); ctr < cnt;
       ctr++) {
    os << child_spacer << children_[ctr].get()->GetPlanNodeType() << "\n";
    os << children_[ctr].get()->GetInfo();
  }

  return os.str();
}

/**
 * This function replaces all COLUMN_REF expressions with TupleValue expressions
 */
void AbstractPlan::ReplaceColumnExpressions(
    catalog::Schema *schema, expression::AbstractExpression *expression) {
  LOG_INFO("Expression Type --> %s",
           ExpressionTypeToString(expression->GetExpressionType()).c_str());
  if (expression->GetLeft() == nullptr) return;
  LOG_INFO("Left Type --> %s",
           ExpressionTypeToString(expression->GetLeft()->GetExpressionType())
               .c_str());
  if (expression->GetRight() == nullptr) return;
  LOG_INFO("Right Type --> %s",
           ExpressionTypeToString(expression->GetRight()->GetExpressionType())
               .c_str());
  if (expression->GetLeft()->GetExpressionType() ==
      EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetLeft();
    std::string col_name(expr->GetName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setLeftExpression(
        expression::ExpressionUtil::ConvertToTupleValueExpression(schema,
                                                                  col_name));
  } else if (expression->GetRight()->GetExpressionType() ==
             EXPRESSION_TYPE_COLUMN_REF) {
    auto expr = expression->GetRight();
    std::string col_name(expr->GetName());
    LOG_INFO("Column name: %s", col_name.c_str());
    delete expr;
    expression->setRightExpression(
        expression::ExpressionUtil::ConvertToTupleValueExpression(schema,
                                                                  col_name));
  } else {
    ReplaceColumnExpressions(schema, expression->GetModifiableLeft());
    ReplaceColumnExpressions(schema, expression->GetModifiableRight());
  }
}

}  // namespace planner
}  // namespace peloton
