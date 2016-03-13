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
        casetype(vt),
        defresult(defresult) {}

  virtual ~CaseExpression() {
    for (auto clause : *clauses) delete clause;
    delete clauses;
    delete defresult;
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
    return "To be implemented";
  }

 private:
  // Arguments
  std::vector<AbstractExpression *> *clauses;
  AbstractExpression *defresult;  // default result
  ValueType casetype;
};

}  // End expression namespace
}  // End peloton namespace

