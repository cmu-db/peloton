//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// udf_expression.h
//
// Identification: src/backend/expression/udf_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/expression/abstract_expression.h"
#include "nodes/execnodes.h"
#include "backend/bridge/dml/tuple/tuple_transformer.h"
#include "fmgr.h"

namespace peloton {
namespace expression {


class UDFExpression : public AbstractExpression {
private:
  Oid func_id;
  Oid collation;
  Oid return_type;
  const std::vector<expression::AbstractExpression*> &m_args;

public:
  UDFExpression(Oid id, Oid col, Oid rettype, const std::vector<expression::AbstractExpression*> &args)
    : AbstractExpression(EXPRESSION_TYPE_FUNCTION), m_args(args) {
    func_id = id;
    collation = col;
    return_type = rettype;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
//    assert(m_left);

    std::vector<Datum> args_eval(m_args.size());
    for(size_t i = 0; i < m_args.size(); i++) {
      args_eval[i] = bridge::TupleTransformer::GetDatum(m_args[i]->Evaluate(tuple1, tuple2, context));
    }

    Datum result;
    switch (m_args.size()) {
      case 0:
        result = OidFunctionCall0Coll(func_id, collation);
        break;
      case 1:
        result = OidFunctionCall1Coll(func_id, collation, args_eval[0]);
        break;
      case 2:
        result = OidFunctionCall2Coll(func_id, collation, args_eval[0], args_eval[1]);
        break;
      case 3:
        break;
      default:
        break;
    }

    auto result_value = bridge::TupleTransformer::GetValue(result, return_type);
    return result_value;
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "UDFExpression");
  }

  AbstractExpression *Copy() const {
    return new UDFExpression(func_id, collation, return_type, m_args);
  }
};

}  // End expression namespace
}  // End peloton namespace
