//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// vector_expression.h
//
// Identification: src/backend/expression/case_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/expression/abstract_expression.h"
#include "backend/expression/expression_util.h"

namespace peloton {
namespace expression {

class CaseExpression : public AbstractExpression {

 public:
  CaseExpression(ValueType vt, std::vector<AbstractExpression *> *clauses,
                 AbstractExpression *defresult)
      : AbstractExpression(EXPRESSION_TYPE_OPERATOR_CASE_EXPR),
        clauses(clauses),
        defresult(defresult),
        case_type(vt) {}

  ~CaseExpression() {
    if(clauses != nullptr){
      for (auto clause : *clauses)
        delete clause;
    }

    delete clauses;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    for (auto clause : *clauses) {
      try {
        return clause->Evaluate(tuple1, tuple2, context);
      } catch (int ExceptionCode) {
        if (ExceptionCode == 0) continue;
        throw Exception("Unknown Exception code in CaseExpression.");
      }
    }
    return defresult->Evaluate(tuple1, tuple2, context);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return spacer + "CaseExpression";
  }

 private:
  // Arguments
  std::vector<AbstractExpression *> *clauses;

  // default result
  AbstractExpression *defresult;

  ValueType case_type;
};

}  // End expression namespace
}  // End peloton namespace
