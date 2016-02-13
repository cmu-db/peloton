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
    return (spacer + "OperatorSubstringExpression");
  }
};

class ConcatExpression : public AbstractExpression {

private:

 public:
	ConcatExpression(AbstractExpression *lc, AbstractExpression*rc)
      : AbstractExpression(EXPRESSION_TYPE_CONCAT){
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
    return (spacer + "OperatorConcatExpression");
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
      : AbstractExpression(EXPRESSION_TYPE_OCTET_LEN){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_OCTET_LENGTH>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorOctetLengthExpression");
  }
};

class CharExpression : public AbstractExpression {

private:

 public:
	CharExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR){
		m_left=lc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_CHAR>();

  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorCharExpression");
  }
};

class CharLengthExpression : public AbstractExpression {

private:

 public:
	CharLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR_LEN){
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

class RepeatExpression : public AbstractExpression {

private:

 public:
	RepeatExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_REPEAT){
		m_left=lc;
		m_right=rc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    assert(m_right);
    std::vector<Value> repeat_args;
    repeat_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    repeat_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_REPEAT>(repeat_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorRepeatExpression");
  }
};

class LeftExpression : public AbstractExpression {

private:

 public:
	LeftExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_LEFT){
		m_left=lc;
		m_right=rc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    assert(m_right);
    std::vector<Value> left_args;
    left_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    left_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_LEFT>(left_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorLeftExpression");
  }
};

class RightExpression : public AbstractExpression {

private:

 public:
	RightExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_RIGHT){
		m_left=lc;
		m_right=rc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    assert(m_right);
    std::vector<Value> right_args;
    right_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    right_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_RIGHT>(right_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorRightExpression");
  }
};

class LTrimExpression : public AbstractExpression{
public:
	LTrimExpression(AbstractExpression * chars, AbstractExpression *string) :
		AbstractExpression(EXPRESSION_TYPE_RTRIM){
		m_left = chars;
		m_right = string;
	}

	  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
	                 executor::ExecutorContext *context) const {
	    assert(m_left);
	    assert(m_right);
	    std::vector<Value> position_args;
	    position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
	    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
	    return Value::Call<FUNC_TRIM_LEADING_CHAR>(position_args);
	  }

	  std::string DebugInfo(const std::string &spacer) const {
	      return (spacer + "OperatorLTrimExpression");
	    }
};

class RTrimExpression : public AbstractExpression{
public:
	RTrimExpression(AbstractExpression * chars, AbstractExpression *string) :
		AbstractExpression(EXPRESSION_TYPE_RTRIM){
		m_left = chars;
		m_right = string;
	}

	  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
	                 executor::ExecutorContext *context) const {
	    assert(m_left);
	    assert(m_right);
	    std::vector<Value> position_args;
	    position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
	    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
	    return Value::Call<FUNC_TRIM_TRAILING_CHAR>(position_args);
	  }

	  std::string DebugInfo(const std::string &spacer) const {
	      return (spacer + "OperatorRTrimExpression");
	    }
};

class BTrimExpression : public AbstractExpression{
public:
	BTrimExpression(AbstractExpression * chars, AbstractExpression *string) :
		AbstractExpression(EXPRESSION_TYPE_BTRIM){
		m_left = chars;
		m_right = string;
	}

	  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
	                 executor::ExecutorContext *context) const {
	    assert(m_left);
	    assert(m_right);
	    std::vector<Value> position_args;
	    position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
	    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
	    return Value::Call<FUNC_TRIM_BOTH_CHAR>(position_args);
	  }

	  std::string DebugInfo(const std::string &spacer) const {
	      return (spacer + "OperatorBTrimExpression");
	    }
};

class PositionExpression : public AbstractExpression {

private:

 public:
	PositionExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_POSITION){
		m_left=lc;
		m_right=rc;
	};


  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    assert(m_right);
    std::vector<Value> position_args;
    position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_POSITION_CHAR>(position_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "OperatorPositionExpression");
  }
};

class OverlayExpression : public AbstractExpression {

private:
  AbstractExpression *from;
  AbstractExpression *len;
 public:
  OverlayExpression(AbstractExpression *string1, AbstractExpression *string2, AbstractExpression *from, AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_OVERLAY), from(from), len(len) {
	  m_left = string1;
	  m_right = string2;
  };

  ~OverlayExpression(){
	delete from;
    delete len;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    std::vector<Value> overlay_args;
    overlay_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    overlay_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    overlay_args.emplace_back(from->Evaluate(tuple1, tuple2, context));
    if (len != nullptr){
    	overlay_args.emplace_back(len->Evaluate(tuple1, tuple2, context));
    }
	return Value::Call<FUNC_OVERLAY_CHAR>(overlay_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
      return (spacer + "OperatorOverlayExpression");
    }
};

class ReplaceExpression : public AbstractExpression {

private:
  AbstractExpression *to;
 public:
  ReplaceExpression(AbstractExpression *string, AbstractExpression *from, AbstractExpression *to)
      : AbstractExpression(EXPRESSION_TYPE_REPLACE), to(to){
	  m_left = string;
	  m_right = from;
  };

  ~ReplaceExpression(){
	delete to;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    std::vector<Value> replace_args;
    replace_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    replace_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    replace_args.emplace_back(to->Evaluate(tuple1, tuple2, context));
	return Value::Call<FUNC_REPLACE>(replace_args);
  }

  std::string DebugInfo(const std::string &spacer) const {
      return (spacer + "OperatorReplaceExpression");
    }
};



}  // End expression namespace
}  // End peloton namespace
