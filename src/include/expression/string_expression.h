#pragma once

#include <string>
#include <vector>

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

// ASCII code of the first character of the argument.
class AsciiExpression : public AbstractExpression {
 public:
  AsciiExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_ASCII, Type::INTEGER, lc, nullptr) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    Value vl = left_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    int32_t val = str[0];
    return ValueFactory::GetIntegerValue(val);
  }

  AbstractExpression *Copy() const override {
    return new AsciiExpression(left_->Copy());
  }
};

class ChrExpression : public AbstractExpression {
 public:
  ChrExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR, Type::VARCHAR, lc, nullptr) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    Value vl = left_->Evaluate(tuple1, tuple2, context);
    int32_t val = vl.GetAs<int32_t>();
    std::string str(1, char(static_cast<char>(val)));
    return ValueFactory::GetVarcharValue(str);
  }

  AbstractExpression *Copy() const override {
    return new ChrExpression(left_->Copy());
  }
};

class SubstrExpression : public AbstractExpression {
 public:
  SubstrExpression(AbstractExpression *lc,
                   AbstractExpression *rc,
                   AbstractExpression *len)
      : AbstractExpression(EXPRESSION_TYPE_SUBSTR, Type::VARCHAR, lc, rc) {
    len_ = len;
  }

  ~SubstrExpression() {
    delete len_;
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    int32_t from = vr.GetAs<int32_t>() - 1;
    int32_t len = (len_->Evaluate(nullptr, nullptr, nullptr)).GetAs<int32_t>();
    return ValueFactory::GetVarcharValue(str.substr(from, len));
  }

  AbstractExpression *Copy() const override {
    return new SubstrExpression(left_->Copy(), right_->Copy(), len_->Copy());
  }

 private:
  AbstractExpression *len_;
};

// Number of characters in string
class CharLengthExpression : public AbstractExpression {
 public:
  CharLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_CHAR_LEN,
                           Type::INTEGER, lc, nullptr) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    int32_t len = str.length();
    return (ValueFactory::GetIntegerValue(len));
  }

  AbstractExpression *Copy() const override {
    return new CharLengthExpression(left_->Copy());
  }
};

class ConcatExpression : public AbstractExpression {
 public:
  ConcatExpression(AbstractExpression *lc,
                   AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_CONCAT, Type::VARCHAR, lc, rc) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString() + vr.ToString();
    return (ValueFactory::GetVarcharValue(str));
  }

  AbstractExpression *Copy() const override {
    return new ConcatExpression(left_->Copy(), right_->Copy());
  }
};

// Number of bytes in string
class OctetLengthExpression : public AbstractExpression {
 public:
  OctetLengthExpression(AbstractExpression *lc)
      : AbstractExpression(EXPRESSION_TYPE_OCTET_LEN,
                           Type::INTEGER, lc, nullptr) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    int32_t len = str.length();
    return (ValueFactory::GetIntegerValue(len));
  }

  AbstractExpression *Copy() const override {
    return new OctetLengthExpression(left_->Copy());
  }
};

// Repeat string the specified number of times
class RepeatExpression : public AbstractExpression {
 public:
  RepeatExpression(AbstractExpression *lc,
                   AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_REPEAT, Type::VARCHAR, lc, rc) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    int32_t num = vr.GetAs<int32_t>();
    std::string ret = "";
    while (num--) {
      ret = ret + str;
    }
    return (ValueFactory::GetVarcharValue(ret));
  }

  AbstractExpression *Copy() const override {
    return new RepeatExpression(left_->Copy(), right_->Copy());
  }
};

// Replace all occurrences in string of substring from with substring to
class ReplaceExpression : public AbstractExpression {
 public:
  ReplaceExpression(AbstractExpression *lc,
                    AbstractExpression *rc,
                    AbstractExpression *to)
      : AbstractExpression(EXPRESSION_TYPE_REPLACE, Type::VARCHAR, lc, rc) {
    to_ = to;
  }

  ~ReplaceExpression() {
    delete to_;
  }

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    std::string from = vr.ToString();
    std::string to = (to_->Evaluate(nullptr, nullptr, nullptr)).ToString();
    size_t pos = 0;
    while((pos = str.find(from, pos)) != std::string::npos) {
      str.replace(pos, from.length(), to);
      pos += to.length();
    }
    return (ValueFactory::GetVarcharValue(str));
  }

  AbstractExpression *Copy() const override {
    return new ReplaceExpression(left_->Copy(), right_->Copy(), to_->Copy());
  }

 private:
  AbstractExpression *to_;
};

// Remove the longest string containing only characters from characters
// from the start of string
class LTrimExpression : public AbstractExpression {
 public:
  LTrimExpression(AbstractExpression *lc,
                  AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_LTRIM, Type::VARCHAR, lc, rc) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    std::string from = vr.ToString();
    size_t pos = 0;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos++;
    }
    if (erase)
      str.erase(0, pos);
    return (ValueFactory::GetVarcharValue(str));
  }

  AbstractExpression *Copy() const override {
    return new LTrimExpression(left_->Copy(), right_->Copy());
  }
};

// Remove the longest string containing only characters from characters
// from the end of string
class RTrimExpression : public AbstractExpression {
 public:
  RTrimExpression(AbstractExpression *lc,
                  AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_RTRIM, Type::VARCHAR, lc, rc) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    std::string from = vr.ToString();
    if (str.length() == 0)
      return (ValueFactory::GetVarcharValue(""));
    size_t pos = str.length() - 1;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos--;
    }
    if (erase)
      str.erase(pos + 1, str.length() - pos - 1);
    return (ValueFactory::GetVarcharValue(str));
  }

  AbstractExpression *Copy() const override {
    return new RTrimExpression(left_->Copy(), right_->Copy());
  }
};

// Remove the longest string consisting only of characters in characters
// from the start and end of string
class BTrimExpression : public AbstractExpression {
 public:
  BTrimExpression(AbstractExpression *lc,
                  AbstractExpression *rc)
      : AbstractExpression(EXPRESSION_TYPE_BTRIM, Type::VARCHAR, lc, rc) {}

  Value Evaluate(UNUSED_ATTRIBUTE const AbstractTuple *tuple1,
      UNUSED_ATTRIBUTE const AbstractTuple *tuple2,
      UNUSED_ATTRIBUTE executor::ExecutorContext *context) const override {
    auto vl = left_->Evaluate(tuple1, tuple2, context);
    auto vr = right_->Evaluate(tuple1, tuple2, context);
    std::string str = vl.ToString();
    std::string from = vr.ToString();

    if (str.length() == 0)
      return (ValueFactory::GetVarcharValue(""));

    size_t pos = str.length() - 1;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos--;
    }
    if (erase)
      str.erase(pos + 1, str.length() - pos - 1);
    
    pos = 0;
    erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos++;
    }
    if (erase)
      str.erase(0, pos);
    return (ValueFactory::GetVarcharValue(str));
  }

  AbstractExpression *Copy() const override {
    return new BTrimExpression(left_->Copy(), right_->Copy());
  }
};

}  // namespace expression
}  // namespace peloton
