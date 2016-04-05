//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// date_expression.h
//
// Identification: src/backend/expression/date_expression.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/serializer.h"

#include "backend/common/value.h"
#include "backend/common/value_peeker.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/string_functions.h"

namespace peloton {
namespace expression {

enum TimestampSubfield {
  YEAR,
  MONTH,
  DAY,
  DAY_OF_WEEK,
  WEEKDAY,
  WEEK_OF_YEAR,
  DAY_OF_YEAR,
  QUARTER,
  HOUR,
  MINUTE,
  SECOND,
  NOT_CONSTANT
};

/*
 * Extract Expression
 */
class ExtractExpression : public AbstractExpression {
 public:
  /*
   * constructor
   * subfield: field to extract
   * date: date or timestamp to extract from
   */
  ExtractExpression(AbstractExpression *subfield, AbstractExpression *date)
      : AbstractExpression(EXPRESSION_TYPE_EXTRACT, subfield, date) {
    if (subfield->GetExpressionType() == EXPRESSION_TYPE_VALUE_CONSTANT) {
      this->subfield = ExtractExpression::GetFieldFromValue(
          subfield->Evaluate(nullptr, nullptr, nullptr));
    } else {
      this->subfield = NOT_CONSTANT;
    }
  }

  ~ExtractExpression() {}

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    assert(m_left);
    assert(m_right);
    TimestampSubfield local_subfield;

    if (subfield == NOT_CONSTANT) {
      local_subfield =
          GetFieldFromValue(m_left->Evaluate(tuple1, tuple2, context));
    } else {
      local_subfield = subfield;
    }
    Value timestamp = m_right->Evaluate(tuple1, tuple2, context);
    Value ret;
    switch (local_subfield) {
      case YEAR:
        ret = timestamp.CallUnary<FUNC_EXTRACT_YEAR>();
        break;
      case MONTH:
        ret = timestamp.CallUnary<FUNC_EXTRACT_MONTH>();
        break;
      case DAY:
        ret = timestamp.CallUnary<FUNC_EXTRACT_DAY>();
        break;
      case DAY_OF_WEEK:
        ret = timestamp.CallUnary<FUNC_EXTRACT_DAY_OF_WEEK>();
        break;
      case WEEKDAY:
        ret = timestamp.CallUnary<FUNC_EXTRACT_WEEKDAY>();
        break;
      case DAY_OF_YEAR:
        ret = timestamp.CallUnary<FUNC_EXTRACT_DAY_OF_YEAR>();
        break;
      case WEEK_OF_YEAR:
        ret = timestamp.CallUnary<FUNC_EXTRACT_WEEK_OF_YEAR>();
        break;
      case QUARTER:
        ret = timestamp.CallUnary<FUNC_EXTRACT_QUARTER>();
        break;
      case HOUR:
        ret = timestamp.CallUnary<FUNC_EXTRACT_HOUR>();
        break;
      case MINUTE:
        ret = timestamp.CallUnary<FUNC_EXTRACT_MINUTE>();
        break;
      case SECOND:
        ret = timestamp.CallUnary<FUNC_EXTRACT_SECOND>();
        break;
      default:
        ret = timestamp;
    }
    return ret;
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "ExtractExpression");
  }

  AbstractExpression *Copy() const {
    return new ExtractExpression(CopyUtil(GetLeft()), CopyUtil(GetRight()));
  }

 private:
  TimestampSubfield subfield;
  static std::string const YEAR_STR;

  static std::string const MONTH_STR;
  static std::string const DAY_STR;
  static std::string const DAY_OF_WEEK_STR;
  static std::string const WEEKDAY_STR;
  static std::string const WEEK_OF_YEAR_STR;

  static std::string const DAY_OF_YEAR_STR;
  static std::string const QUARTER_STR;
  static std::string const HOUR_STR;
  static std::string const MINUTE_STR;
  static std::string const SECOND_STR;

  static TimestampSubfield GetFieldFromValue(Value val) {
    std::string subfield_str = ValuePeeker::PeekStringCopyWithoutNull(val);
    if (subfield_str == YEAR_STR) {
      return YEAR;
    } else if (subfield_str == MONTH_STR) {
      return MONTH;
    } else if (subfield_str == DAY_STR) {
      return DAY;
    } else if (subfield_str == DAY_OF_WEEK_STR) {
      return DAY_OF_WEEK;
    } else if (subfield_str == WEEKDAY_STR) {
      return WEEKDAY;
    } else if (subfield_str == WEEK_OF_YEAR_STR) {
      return WEEK_OF_YEAR;
    } else if (subfield_str == DAY_OF_YEAR_STR) {
      return DAY_OF_YEAR;
    } else if (subfield_str == QUARTER_STR) {
      return QUARTER;
    } else if (subfield_str == HOUR_STR) {
      return HOUR;
    } else if (subfield_str == MINUTE_STR) {
      return MINUTE;
    } else if (subfield_str == SECOND_STR) {
      return SECOND;
    } else {
      return NOT_CONSTANT;
    }
  }
};

const std::string ExtractExpression::YEAR_STR = "year";
const std::string ExtractExpression::MONTH_STR = "month";
const std::string ExtractExpression::DAY_STR = "day";
const std::string ExtractExpression::DAY_OF_WEEK_STR = "dow";
const std::string ExtractExpression::WEEKDAY_STR = "weekday";
const std::string ExtractExpression::WEEK_OF_YEAR_STR = "woy";

const std::string ExtractExpression::DAY_OF_YEAR_STR = "doy";
const std::string ExtractExpression::QUARTER_STR = "quarter";
const std::string ExtractExpression::HOUR_STR = "hour";
const std::string ExtractExpression::MINUTE_STR = "minute";
const std::string ExtractExpression::SECOND_STR = "second";

//===--------------------------------------------------------------------===//
// An expression that converts a DATE type to a TIMESTAMP type
//===--------------------------------------------------------------------===//
class DateToTimestampExpression : public AbstractExpression {
 public:
  DateToTimestampExpression(AbstractExpression *date_expr)
      : AbstractExpression(EXPRESSION_TYPE_DATE_TO_TIMESTAMP, date_expr,
                           nullptr) {
    assert(GetLeft() != nullptr);
    assert(GetRight() == nullptr);
  }

  Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const {
    Value date = GetLeft()->Evaluate(tuple1, tuple2, context);
    assert(date.GetValueType() == VALUE_TYPE_DATE);
    return date.CallUnary<FUNC_TO_TIMESTAMP_DAY>();
  }

  std::string DebugInfo(const std::string &spacer) const {
    return (spacer + "DateToTimestampExpression");
  }

  AbstractExpression *Copy() const {
    return new DateToTimestampExpression(CopyUtil(GetLeft()));
  }
};

}  // namespace expression
}  // namespace peloton
