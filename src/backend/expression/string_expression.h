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
  AbstractExpression *len;
 public:
  SubstringExpression(AbstractExpression *string, AbstractExpression *from, AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_SUBSTR), len(len) {
	  m_left = string;
	  m_right = from;
  };

  ~SubstringExpression(){
    delete len;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    std::vector<Value> substr_args;
    substr_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    substr_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
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

class ConcatExpression : public AbstractExpression {

private:

 public:
	ConcatExpression(AbstractExpression *lc, AbstractExpression*rc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII){
		m_left=lc;
		m_right=rc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    std::vector<Value> concat_args;
    concat_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    concat_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_CONCAT>(concat_args);

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorAsciiExpression");
  }
};

class AsciiExpression : public AbstractExpression {

private:

 public:
	AsciiExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_ASCII>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorAsciiExpression");
  }
};

class OctetLengthExpression : public AbstractExpression {

private:

 public:
	OctetLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_OCTET_LENGTH>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorAsciiExpression");
  }
};

class CharExpression : public AbstractExpression {

private:

 public:
	CharExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_CHAR>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorAsciiExpression");
  }
};

class CharLengthExpression : public AbstractExpression {

private:

 public:
	CharLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_CHAR_LENGTH>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorCharLengthExpression");
  }
};

class SpaceExpression : public AbstractExpression {

private:

 public:
	SpaceExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_SPACE){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_SPACE>();
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorSpaceExpression");
  }
};





}  // End expression namespace
}  // End peloton namespace
