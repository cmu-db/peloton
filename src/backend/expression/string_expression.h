//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_expression.h
//
// Identification: src/backend/expression/string_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "backend/common/serializer.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/string_functions.h"

namespace peloton {
namespace expression {

/*
 * Substring expressoin
 */

class SubstringExpression : public AbstractExpression {
 public:
  /*
   * constructor
   * string : string to take substring
   * from : first position
   * len : last position
   */
  SubstringExpression(AbstractExpression *string, AbstractExpression *from,
                      AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_SUBSTR, VALUE_TYPE_VARCHAR, string,
                           from),
        len_(len) {}

  ~SubstringExpression() { delete len_; }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    std::vector<Value> substr_args;
    substr_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    substr_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    // if length not defined call 2 arg substring
    if (len_ == nullptr) {
      return Value::Call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(substr_args);
    } else {
      substr_args.emplace_back(len_->Evaluate(tuple1, tuple2, context));
      return Value::Call<FUNC_SUBSTRING_CHAR>(substr_args);
    }
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorSubstringExpression");
  }

  AbstractExpression *Copy() const override {
    return new SubstringExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()),
                                   CopyUtil(len_));
  }

 private:
  AbstractExpression *len_;  // length of substring to take (optional)
};

/*
 * Implements the 2 variable concat expression
 */
class ConcatExpression : public AbstractExpression {
 private:
 public:
  ConcatExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_CONCAT, VALUE_TYPE_VARCHAR, lc, rc)
  {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    std::vector<Value> concat_args;
    concat_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    concat_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_CONCAT>(concat_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorConcatExpression");
  }

  AbstractExpression *Copy() const override {
    return new ConcatExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * Implements the ascii expression
 * Currently does not handle unicode well
 */
class AsciiExpression : public AbstractExpression {
 private:
 public:
  AsciiExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII, VALUE_TYPE_VARCHAR, lc,
                           nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_ASCII>();
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorAsciiExpression");
  }

  AbstractExpression *Copy() const override {
    return new AsciiExpression(CopyUtil(GetLeft()));
  }
};

/*
 * Implements the octet length expression
 */
class OctetLengthExpression : public AbstractExpression {
 private:
 public:
  OctetLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_OCTET_LEN, VALUE_TYPE_INTEGER, lc,
                           nullptr) {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    return m_left->Evaluate(tuple1, tuple2, context)
        .CallUnary<FUNC_OCTET_LENGTH>();
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorOctetLengthExpression");
  }

  AbstractExpression *Copy() const override {
    return new OctetLengthExpression(CopyUtil(GetLeft()));
  }
};

/*
 * Implements the CHR expression
 */
class CharExpression : public AbstractExpression {
 private:
 public:
  CharExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR, VALUE_TYPE_VARCHAR) {
    m_left = lc;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    return m_left->Evaluate(tuple1, tuple2, context).CallUnary<FUNC_CHAR>();
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorCharExpression");
  }

  AbstractExpression *Copy() const override {
    return new CharExpression(CopyUtil(GetLeft()));
  }
};

/*
 * Implements the CHAR LENGTH Expression
 */
class CharLengthExpression : public AbstractExpression {
 private:
 public:
  CharLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR_LEN, VALUE_TYPE_BIGINT) {
    m_left = lc;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    return m_left->Evaluate(tuple1, tuple2, context)
        .CallUnary<FUNC_CHAR_LENGTH>();
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorCharLengthExpression");
  }

  AbstractExpression *Copy() const override {
    return new CharLengthExpression(CopyUtil(GetLeft()));
  }
};

/*
 * implements the repeat function
 */
class RepeatExpression : public AbstractExpression {
 private:
 public:
  RepeatExpression(AbstractExpression *string, AbstractExpression *num)
      : AbstractExpression(EXPRESSION_TYPE_REPEAT, VALUE_TYPE_VARCHAR) {
    m_left = string;
    m_right = num;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    ALWAYS_ASSERT(m_right);
    std::vector<Value> repeat_args;
    repeat_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    repeat_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_REPEAT>(repeat_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorRepeatExpression");
  }

  AbstractExpression *Copy() const override {
    return new RepeatExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implements left
 */
class LeftExpression : public AbstractExpression {
 private:
 public:
  LeftExpression(AbstractExpression *string, AbstractExpression *num)
      : AbstractExpression(EXPRESSION_TYPE_LEFT, VALUE_TYPE_VARCHAR) {
    m_left = string;
    m_right = num;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    ALWAYS_ASSERT(m_right);
    std::vector<Value> left_args;
    left_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    left_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_LEFT>(left_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorLeftExpression");
  }

  AbstractExpression *Copy() const override {
    return new LeftExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implements right
 */
class RightExpression : public AbstractExpression {
 private:
 public:
  RightExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_RIGHT, VALUE_TYPE_VARCHAR) {
    m_left = lc;
    m_right = rc;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    ALWAYS_ASSERT(m_right);
    std::vector<Value> right_args;
    right_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    right_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_RIGHT>(right_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorRightExpression");
  }

  AbstractExpression *Copy() const override {
    return new RightExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implements ltrim (leading trim)
 */
class LTrimExpression : public AbstractExpression {
 public:
  LTrimExpression(AbstractExpression *string, AbstractExpression *chars)
      : AbstractExpression(EXPRESSION_TYPE_RTRIM, VALUE_TYPE_VARCHAR) {
    m_left = string;
    m_right = chars;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    std::vector<Value> position_args;
    // if null do not add to the arguments
    if (m_left) {
      position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    }
    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));

    return Value::Call<FUNC_TRIM_LEADING_CHAR>(position_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorLTrimExpression");
  }

  AbstractExpression *Copy() const override {
    return new LTrimExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implements rtrim (trailing trim)
 */
class RTrimExpression : public AbstractExpression {
 public:
  RTrimExpression(AbstractExpression *string, AbstractExpression *chars)
      : AbstractExpression(EXPRESSION_TYPE_RTRIM, VALUE_TYPE_VARCHAR) {
    m_left = string;
    m_right = chars;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    std::vector<Value> position_args;
    // if null do not add to the arguments
    if (m_left) {
      position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    }
    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_TRIM_TRAILING_CHAR>(position_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorRTrimExpression");
  }

  AbstractExpression *Copy() const override {
    return new RTrimExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

class BTrimExpression : public AbstractExpression {
 public:
  BTrimExpression(AbstractExpression *string, AbstractExpression *chars)
      : AbstractExpression(EXPRESSION_TYPE_BTRIM, VALUE_TYPE_VARCHAR) {
    m_left = string;
    m_right = chars;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    std::vector<Value> position_args;
    // if null do not add to the arguments
    if (m_left) {
      position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    }
    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));

    return Value::Call<FUNC_TRIM_BOTH_CHAR>(position_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorBTrimExpression");
  }

  AbstractExpression *Copy() const override {
    return new BTrimExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implement position
 */
class PositionExpression : public AbstractExpression {
 private:
 public:
  PositionExpression(AbstractExpression *lc, AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_POSITION, VALUE_TYPE_INTEGER) {
    m_left = lc;
    m_right = rc;
  };

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    ALWAYS_ASSERT(m_right);
    std::vector<Value> position_args;
    position_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    position_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));

    return Value::Call<FUNC_POSITION_CHAR>(position_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorPositionExpression");
  }

  AbstractExpression *Copy() const override {
    return new PositionExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }
};

/*
 * implements overlay (4 args)
 */
class OverlayExpression : public AbstractExpression {
 private:
  AbstractExpression *from;
  AbstractExpression *len;

 public:
  OverlayExpression(AbstractExpression *string1, AbstractExpression *string2,
                    AbstractExpression *from, AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_OVERLAY, VALUE_TYPE_VARCHAR),
        from(from),
        len(len) {
    m_left = string1;
    m_right = string2;
  };

  ~OverlayExpression() {
    delete from;
    delete len;
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    std::vector<Value> overlay_args;
    overlay_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    overlay_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    overlay_args.emplace_back(from->Evaluate(tuple1, tuple2, context));
    if (len) {
      overlay_args.emplace_back(len->Evaluate(tuple1, tuple2, context));
    }
    return Value::Call<FUNC_OVERLAY_CHAR>(overlay_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorOverlayExpression");
  }

  AbstractExpression *Copy() const override {
    return new OverlayExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()),
                                 CopyUtil(from), CopyUtil(len));
  }
};

/*
 * implements replace
 */
class ReplaceExpression : public AbstractExpression {
 private:
  AbstractExpression *to;

 public:
  ReplaceExpression(AbstractExpression *string, AbstractExpression *from,
                    AbstractExpression *to)
      : AbstractExpression(EXPRESSION_TYPE_REPLACE, VALUE_TYPE_VARCHAR),
        to(to) {
    m_left = string;
    m_right = from;
  };

  ~ReplaceExpression() { delete to; }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    ALWAYS_ASSERT(m_left);
    std::vector<Value> replace_args;
    replace_args.emplace_back(m_left->Evaluate(tuple1, tuple2, context));
    replace_args.emplace_back(m_right->Evaluate(tuple1, tuple2, context));
    replace_args.emplace_back(to->Evaluate(tuple1, tuple2, context));
    return Value::Call<FUNC_REPLACE>(replace_args);
  }

  std::string DebugInfo(const std::string &spacer) const override {
    return (spacer + "OperatorReplaceExpression");
  }

  AbstractExpression *Copy() const override {
    return new ReplaceExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()),
                                 CopyUtil(to));
  }
};

}  // namespace expression
}  // namespace peloton
