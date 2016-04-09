//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// case_expression.h
//
// Identification: src/backend/expression/case_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <utility>
#include <memory>
#include <vector>
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

class CaseExpression : public AbstractExpression {
 public:
  // the first part is condition, and the second part is expression
  typedef std::unique_ptr<AbstractExpression> AbstractExprPtr;
  typedef std::pair<AbstractExprPtr, AbstractExprPtr> WhenClause;
  CaseExpression(ValueType vt, std::vector<WhenClause> &t_clauses,
                 AbstractExprPtr default_clause)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR),
        clauses_(std::move(t_clauses)),
        default_expression_(std::move(default_clause)),
        case_type_(vt) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto &clause : clauses_) {
      auto condition = clause.first->Evaluate(tuple1, tuple2, context);
      if (condition.IsTrue())
        return clause.second->Evaluate(tuple1, tuple2, context);
    }
    return default_expression_->Evaluate(tuple1, tuple2, context);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CaseExpression";
  }

  AbstractExpression *Copy() const {
    std::vector<WhenClause> copied_clauses;
    for (auto &clause : clauses_) {
      copied_clauses.push_back(
          WhenClause(AbstractExprPtr(clause.first->Copy()),
                     AbstractExprPtr(clause.second->Copy())));
    }
    // default result has no condition.
    return new CaseExpression(case_type_, copied_clauses,
                              AbstractExprPtr(default_expression_->Copy()));
  }

 private:
  // Case expression clauses
  std::vector<WhenClause> clauses_;

  // Fallback case result expression
  AbstractExprPtr default_expression_;

  ValueType case_type_;
};

}  // End expression namespace
}  // End peloton namespace
