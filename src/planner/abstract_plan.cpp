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

#include "common/logger.h"
#include "common/types.h"
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

void AbstractPlan::SetParameterValues(std::vector<common::Value *> *values) {
  LOG_TRACE("Setting parameter values in all child plans of %s",
            GetInfo().c_str());
  for (auto &child_plan : GetChildren()) {
    child_plan->SetParameterValues(values);
  }
};

/**
 * This function replaces all COLUMN_REF expressions with TupleValue expressions
 */
void AbstractPlan::ReplaceColumnExpressions(
    catalog::Schema *schema, expression::AbstractExpression *expression) {
  LOG_TRACE("Expression Type --> %s",
            ExpressionTypeToString(expression->GetExpressionType()).c_str());
  if (expression->GetLeft() != nullptr) {
    LOG_TRACE("Left Type --> %s",
              ExpressionTypeToString(expression->GetLeft()->GetExpressionType())
                  .c_str());
    if (expression->GetLeft()->GetExpressionType() ==
        EXPRESSION_TYPE_COLUMN_REF) {
      auto expr = expression->GetModifiableLeft();
      std::string col_name(expr->GetName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      delete expr;
      expression->setLeftExpression(
          expression::ExpressionUtil::ConvertToTupleValueExpression(schema,
                                                                    col_name));

    } else {
      ReplaceColumnExpressions(schema, expression->GetModifiableLeft());
    }
  }

  if (expression->GetRight() != nullptr) {
    LOG_TRACE(
        "Right Type --> %s",
        ExpressionTypeToString(expression->GetRight()->GetExpressionType())
            .c_str());
    if (expression->GetRight()->GetExpressionType() ==
        EXPRESSION_TYPE_COLUMN_REF) {
      auto expr = expression->GetModifiableRight();
      std::string col_name(expr->GetName());
      LOG_TRACE("Column name: %s", col_name.c_str());
      delete expr;
      expression->setRightExpression(
          expression::ExpressionUtil::ConvertToTupleValueExpression(schema,
                                                                    col_name));
    } else {
      ReplaceColumnExpressions(schema, expression->GetModifiableRight());
    }
  }
}

}  // namespace planner
}  // namespace peloton
