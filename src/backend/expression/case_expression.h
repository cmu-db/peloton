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
#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

// the first part is condition, and the second part is expression
typedef std::pair<AbstractExpression *, AbstractExpression *> WhenClause;

class CaseExpression : public AbstractExpression {
 public:
  CaseExpression(ValueType vt, const std::vector<WhenClause *> &clauses,
                 WhenClause *default_clause)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR),
        clauses(clauses),
        default_clause(default_clause),
        case_type(vt) {}

  ~CaseExpression() {
    for (auto clause : clauses) {
      delete clause->first;
      clause->first = nullptr;
      delete clause->second;
      clause->second = nullptr;
      delete clause;
      clause = nullptr;
    }
    if (default_clause != nullptr) {
      delete default_clause->first;
      default_clause->first = nullptr;
      delete default_clause->second;
      default_clause->second = nullptr;
      delete default_clause;
      default_clause = nullptr;
    }
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto clause : clauses) {
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
    std::vector<WhenClause *> copied_clauses;
    for (auto clause : clauses) {
      copied_clauses.push_back(new WhenClause(clause->first, clause->second));
    }
    // default result has no condition.
    return new CaseExpression(case_type, copied_clauses,
                              new WhenClause(nullptr, default_clause->second));
  }

 private:
  // Case expression clauses
  std::vector<WhenClause *> clauses;

  // Fallback case result expression
  WhenClause *default_clause;

  ValueType case_type;
};

}  // End expression namespace
}  // End peloton namespace
