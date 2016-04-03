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

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

class CaseExpression : public AbstractExpression {
 public:
  CaseExpression(ValueType vt, const std::vector<AbstractExpression *> &clauses,
                 AbstractExpression *default_result)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR),
        clauses(clauses),
        default_result(default_result),
        case_type(vt) {}

  ~CaseExpression() {
    for (auto clause : clauses) {
      delete clause;
      clause = nullptr;
    }
    if (default_result != nullptr) {
      delete default_result;
      default_result = nullptr;
    }
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto clause : clauses) {
      try {
        return clause->Evaluate(tuple1, tuple2, context);
      } catch (int ExceptionCode) {
        if (ExceptionCode == 0) continue;
        throw Exception("Unknown Exception code in CaseExpression.");
      }
    }
    return default_result->Evaluate(tuple1, tuple2, context);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CaseExpression";
  }

    AbstractExpression *Copy() const {
      std::vector<AbstractExpression *> copied_clauses;
      for (AbstractExpression *clause : clauses) {
        if (clause == nullptr) {
          continue;
        }
        copied_clauses.push_back(clause->Copy());
      }

      return new CaseExpression(case_type, copied_clauses,
                                default_result->Copy());
    }

 private:
  // Case expression clauses
  std::vector<AbstractExpression *> clauses;

  // Fallback case result expression
  AbstractExpression *default_result;

  ValueType case_type;
};

}  // End expression namespace
}  // End peloton namespace
