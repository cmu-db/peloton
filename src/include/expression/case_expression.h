//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_expression.h
//
// Identification: src/expression/case_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"

#include <utility>
#include <memory>
#include <vector>

#include "expression/expression_util.h"

namespace peloton {
namespace expression {

class CaseExpression : public AbstractExpression {
 public:
  // the first part is condition, and the second part is expression
  typedef std::unique_ptr<AbstractExpression> AbstractExprPtr;
  typedef std::pair<AbstractExprPtr, AbstractExprPtr> WhenClause;

  CaseExpression(ValueType vt, std::vector<WhenClause> &clauses,
                 AbstractExprPtr default_clause)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR, vt),
        clauses_(std::move(clauses)),
        default_expression_(std::move(default_clause)) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    for (auto &clause : clauses_) {
      auto condition = clause.first->Evaluate(tuple1, tuple2, context);
      if (condition.IsTrue())
        return clause.second->Evaluate(tuple1, tuple2, context);
    }
    return default_expression_->Evaluate(tuple1, tuple2, context);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return spacer + "CaseExpression";
  }

  AbstractExpression *Copy() const override {
    std::vector<WhenClause> copied_clauses;
    for (auto &clause : clauses_) {
      copied_clauses.push_back(
          WhenClause(AbstractExprPtr(clause.first->Copy()),
                     AbstractExprPtr(clause.second->Copy())));
    }
    // default result has no condition.
    return new CaseExpression(GetValueType(), copied_clauses,
                              AbstractExprPtr(default_expression_->Copy()));
  }

 private:
  // Case expression clauses
  std::vector<WhenClause> clauses_;

  // Fallback case result expression
  AbstractExprPtr default_expression_;
};

}  // namespace expression
}  // namespace peloton
