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
  typedef std::pair<AbstractExprPtr, std::unique_ptr<AbstractExpression>>
      WhenClause;
  typedef std::unique_ptr<WhenClause> WhenClausePtr;
  CaseExpression(ValueType vt, std::vector<WhenClausePtr> &t_clauses,
                 WhenClausePtr default_clause)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR),
        default_clause(std::move(default_clause)),
        case_type(vt) {
    for (auto &clause : t_clauses) {
      clauses.push_back(std::move(clause));
    }
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto &clause : clauses) {
      auto condition = clause->first->Evaluate(tuple1, tuple2, context);
      if (condition.IsTrue())
        return clause->second->Evaluate(tuple1, tuple2, context);
    }
    return default_clause->second->Evaluate(tuple1, tuple2, context);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CaseExpression";
  }

  AbstractExpression *Copy() const {
    std::vector<WhenClausePtr> copied_clauses;
    for (auto &clause : clauses) {
      copied_clauses.push_back(WhenClausePtr(
          new WhenClause(AbstractExprPtr(clause->first->Copy()),
                         AbstractExprPtr(clause->second->Copy()))));
    }
    // default result has no condition.
    return new CaseExpression(
        case_type, copied_clauses,
        WhenClausePtr(new WhenClause(
            nullptr, AbstractExprPtr(default_clause->second->Copy()))));
  }

 private:
  // Case expression clauses
  std::vector<WhenClausePtr> clauses;

  // Fallback case result expression
  WhenClausePtr default_clause;

  ValueType case_type;
};

}  // End expression namespace
}  // End peloton namespace
