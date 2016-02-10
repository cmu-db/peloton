//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// operator_expression.h
//
// Identification: src/backend/expression/string_expression.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/serializer.h"

#include "backend/expression/abstract_expression.h"
#include "backend/expression/string_functions.h"

#include <string>
#include <cassert>
#include <vector>

namespace peloton {
namespace expression {

/*
 * Unary operators. (NOT and IS_NULL)
 */

class SubstringExpression : public AbstractExpression {

private:
  AbstractExpression *string;
  AbstractExpression *from;
  AbstractExpression *len;
 public:
  SubstringExpression(AbstractExpression *string, AbstractExpression *from, AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_SUBSTR), string(string), from(from), len(len) {};

  ~SubstringExpression(){
    delete string;
    delete from;
    delete len;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(string);
    std::vector<Value> substr_args;
    substr_args.emplace_back(string->Evaluate(tuple1, tuple2, context));
    substr_args.emplace_back(from->Evaluate(tuple1, tuple2, context));
    if (len == nullptr){
	return Value::Call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(substr_args);
    }else{
	substr_args.emplace_back(len->Evaluate(tuple1, tuple2, context));
	return Value::Call<FUNC_SUBSTRING_CHAR>(substr_args);
    }


  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorNotExpression");
  }
};


class AsciiExpression : public AbstractExpression {

private:

 public:
	AsciiExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII) {
		m_left = lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_ASCII>();
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorNotExpression");
  }
};

}  // End expression namespace
}  // End peloton namespace
