//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_util.cpp
//
// Identification: src/backend/expression/expression_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <sstream>
#include <cstdlib>
#include <stdexcept>

#include "backend/common/value_factory.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/hash_range_expression.h"
#include "backend/expression/operator_expression.h"
#include "backend/expression/comparison_expression.h"
#include "backend/expression/case_expression.h"
#include "backend/expression/conjunction_expression.h"
#include "backend/expression/constant_value_expression.h"
#include "backend/expression/tuple_value_expression.h"
#include "backend/expression/cast_expression.h"
#include "backend/expression/tuple_address_expression.h"
#include "backend/expression/scalar_value_expression.h"
#include "backend/expression/vector_expression.h"
#include "backend/expression/function_expression.h"
#include "backend/expression/subquery_expression.h"
#include "backend/expression/string_expression.h"
#include "backend/expression/date_expression.h"
#include "backend/expression/vector_comparison_expression.h"
#include "backend/expression/nullif_expression.h"
#include <json_spirit.h>

namespace peloton {
namespace expression {

// Parse JSON parameters to create a hash range expression
AbstractExpression *ExpressionUtil::HashRangeFactory(PlannerDomValue obj) {
  PlannerDomValue hashColumnValue = obj.valueForKey("HASH_COLUMN");

  PlannerDomValue rangesArray = obj.valueForKey("RANGES");

  srange_type *ranges = new srange_type[rangesArray.arrayLen()];
  for (int ii = 0; ii < rangesArray.arrayLen(); ii++) {
    PlannerDomValue arrayObject = rangesArray.valueAtIndex(ii);
    PlannerDomValue rangeStartValue = arrayObject.valueForKey("RANGE_START");
    PlannerDomValue rangeEndValue = arrayObject.valueForKey("RANGE_END");

    ranges[ii] = srange_type(rangeStartValue.asInt(), rangeEndValue.asInt());
  }
  return new HashRangeExpression(hashColumnValue.asInt(), ranges,
                                 static_cast<int>(rangesArray.arrayLen()));
}

// Parse JSON parameters to create a subquery expression
AbstractExpression *ExpressionUtil::SubqueryFactory(
    ExpressionType subqueryType, PlannerDomValue obj,
    const std::vector<AbstractExpression *> &args) {
  int subqueryId = obj.valueForKey("SUBQUERY_ID").asInt();
  std::vector<int> paramIdxs;
  if (obj.hasNonNullKey("PARAM_IDX")) {
    PlannerDomValue params = obj.valueForKey("PARAM_IDX");
    size_t paramSize = params.arrayLen();
    paramIdxs.reserve(paramSize);
    if (args.size() != paramSize) {
      throw Exception("subqueryFactory: parameter indexes/tve count .Ismatch");
    }
    for (size_t i = 0; i < paramSize; ++i) {
      int paramIdx = params.valueAtIndex(i).asInt();
      paramIdxs.push_back(paramIdx);
    }
  }
  std::vector<int> otherParamIdxs;
  if (obj.hasNonNullKey("OTHER_PARAM_IDX")) {
    PlannerDomValue otherParams = obj.valueForKey("OTHER_PARAM_IDX");
    int otherParamSize = otherParams.arrayLen();
    otherParamIdxs.reserve(otherParamSize);
    otherParamIdxs.reserve(otherParamSize);
    for (int i = 0; i < otherParamSize; ++i) {
      int paramIdx = otherParams.valueAtIndex(i).asInt();
      otherParamIdxs.push_back(paramIdx);
    }
  }
  return new SubqueryExpression(subqueryType, subqueryId, paramIdxs,
                                otherParamIdxs, args);
}

// Function static helper templated functions to vivify an optimal
//    comparison class
AbstractExpression *SubqueryComparisonFactory(
    __attribute__((unused)) PlannerDomValue obj,
    __attribute__((unused)) ExpressionType c,
    __attribute__((unused)) AbstractExpression *l,
    __attribute__((unused)) AbstractExpression *r) {
  // TODO: Need to fix vector comparison expression
  return nullptr;
  /*
  QuantifierType quantifier = QUANTIFIER_TYPE_NONE;
  if (obj.hasNonNullKey("QUANTIFIER")) {
    quantifier =
        static_cast<QuantifierType>(obj.valueForKey("QUANTIFIER").asInt());
  }
  SubqueryExpression *l_subquery = dynamic_cast<SubqueryExpression *>(l);
  SubqueryExpression *r_subquery = dynamic_cast<SubqueryExpression *>(r);
  if (l_subquery != NULL && r_subquery != NULL) {
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
            return new VectorComparisonExpression<CmpEq, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
            return new VectorComparisonExpression<CmpNe, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
            return new VectorComparisonExpression<CmpLt, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
            return new VectorComparisonExpression<CmpGt, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
            return new VectorComparisonExpression<CmpLte, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
            return new VectorComparisonExpression<CmpGte, TupleExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      default:
        char message[256];
        snprintf(message, 256,
                 "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception(message);
    }
  } else if (l_subquery != NULL) {
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
            return new VectorComparisonExpression<CmpEq, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
            return new VectorComparisonExpression<CmpNe, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
            return new VectorComparisonExpression<CmpLt, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
            return new VectorComparisonExpression<CmpGt, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
            return new VectorComparisonExpression<CmpLte, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
            return new VectorComparisonExpression<CmpGte, TupleExtractor,
                ValueExtractor>(c, l, r,
                                quantifier);
      default:
        char message[256];
        snprintf(message, 256,
                 "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception(message);
    }
  } else {
    assert(r_subquery != NULL);
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
            return new VectorComparisonExpression<CmpEq, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
            return new VectorComparisonExpression<CmpNe, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
            return new VectorComparisonExpression<CmpLt, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
            return new VectorComparisonExpression<CmpGt, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
            return new VectorComparisonExpression<CmpLte, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
            return new VectorComparisonExpression<CmpGte, ValueExtractor,
                TupleExtractor>(c, l, r,
                                quantifier);
      default:
        char message[256];
        snprintf(message, 256,
                 "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception(message);
    }
  }
  */
}

AbstractExpression *GetGeneral(ExpressionType c, AbstractExpression *l,
                               AbstractExpression *r) {
  assert(l);
  assert(r);
  switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
      return new ComparisonExpression<CmpEq>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
      return new ComparisonExpression<CmpNe>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
      return new ComparisonExpression<CmpLt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
      return new ComparisonExpression<CmpGt>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
      return new ComparisonExpression<CmpLte>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
      return new ComparisonExpression<CmpGte>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LIKE):
      return new ComparisonExpression<CmpLike>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTLIKE):
      return new ComparisonExpression<CmpNotLike>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_IN):
      return new ComparisonExpression<CmpIn>(c, l, r);
    default:
      char message[256];
      snprintf(message, 256,
               "Invalid ExpressionType '%s' called"
               " for ComparisonExpression",
               ExpressionTypeToString(c).c_str());
      throw Exception(message);
  }
}

template <typename L, typename R>
AbstractExpression *GetMoreSpecialized(ExpressionType c, L *l, R *r) {
  assert(l);
  assert(r);
  switch (c) {
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
      return new InlinedComparisonExpression<CmpEq, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
      return new InlinedComparisonExpression<CmpNe, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
      return new InlinedComparisonExpression<CmpLt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
      return new InlinedComparisonExpression<CmpGt, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
      return new InlinedComparisonExpression<CmpLte, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
      return new InlinedComparisonExpression<CmpGte, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_LIKE):
      return new InlinedComparisonExpression<CmpLike, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_NOTLIKE):
      return new InlinedComparisonExpression<CmpNotLike, L, R>(c, l, r);
    case (EXPRESSION_TYPE_COMPARE_IN):
      return new InlinedComparisonExpression<CmpIn, L, R>(c, l, r);
    default:
      char message[256];
      snprintf(message, 256,
               "Invalid ExpressionType '%s' called for"
               " ComparisonExpression",
               ExpressionTypeToString(c).c_str());
      throw Exception(message);
  }
}

// convert the enumerated value type into a concrete c type for the
// comparison helper templates.
// TODO: This function should be refactored due to many types we should support.
// By Michael 02/25/2016
AbstractExpression *ExpressionUtil::ComparisonFactory(ExpressionType c,
                                                      AbstractExpression *lc,
                                                      AbstractExpression *rc) {
  assert(lc);

  TupleValueExpression *l_tuple = dynamic_cast<TupleValueExpression *>(lc);

  TupleValueExpression *r_tuple = dynamic_cast<TupleValueExpression *>(rc);

  // more specialization available? Yes, add castexpress by Michael at
  // 02/25/2016
  ConstantValueExpression *l_const =
      dynamic_cast<ConstantValueExpression *>(lc);

  CastExpression *l_cast = dynamic_cast<CastExpression *>(lc);

  switch (c) {
    case EXPRESSION_TYPE_COMPARE_EQUAL:
    case EXPRESSION_TYPE_COMPARE_NOTEQUAL:
    case EXPRESSION_TYPE_COMPARE_GREATERTHAN:
    case EXPRESSION_TYPE_COMPARE_LESSTHAN:
    case EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO:
    case EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO:
    case EXPRESSION_TYPE_COMPARE_LIKE:
    case EXPRESSION_TYPE_COMPARE_NOTLIKE: {
      ConstantValueExpression *r_const =
          dynamic_cast<ConstantValueExpression *>(rc);

      // this will inline getValue(), hooray!
      if (l_const != nullptr &&
          r_const != nullptr) {  // CONST-CONST can it happen?
        return GetMoreSpecialized<ConstantValueExpression,
                                  ConstantValueExpression>(c, l_const, r_const);
      } else if (l_const != nullptr && r_tuple != nullptr) {  // CONST-TUPLE
        return GetMoreSpecialized<ConstantValueExpression,
                                  TupleValueExpression>(c, l_const, r_tuple);
      } else if (l_tuple != nullptr && r_const != nullptr) {  // TUPLE-CONST
        return GetMoreSpecialized<TupleValueExpression,
                                  ConstantValueExpression>(c, l_tuple, r_const);
      } else if (l_tuple != nullptr && r_tuple != nullptr) {  // TUPLE-TUPLE
        return GetMoreSpecialized<TupleValueExpression, TupleValueExpression>(
            c, l_tuple, r_tuple);
      }
    } break;

    case EXPRESSION_TYPE_COMPARE_IN: {
      VectorExpression *r_vector = dynamic_cast<VectorExpression *>(rc);

      // this will inline getValue(), hooray!
      if (l_const != nullptr &&
          r_vector != nullptr) {  // CONST-CONST can it happen?
        return GetMoreSpecialized<ConstantValueExpression, VectorExpression>(
            c, l_const, r_vector);
      } else if (l_const != nullptr && r_tuple != nullptr) {  // CONST-TUPLE
        return GetMoreSpecialized<ConstantValueExpression,
                                  TupleValueExpression>(c, l_const, r_tuple);
      } else if (l_tuple != nullptr && r_vector != nullptr) {  // TUPLE-CONST
        return GetMoreSpecialized<TupleValueExpression, VectorExpression>(
            c, l_tuple, r_vector);
      } else if (l_tuple != nullptr && r_tuple != nullptr) {  // TUPLE-TUPLE
        return GetMoreSpecialized<TupleValueExpression, TupleValueExpression>(
            c, l_tuple, r_tuple);
      } else if (l_cast != nullptr &&
                 r_vector != nullptr) {  // CAST-VECTOR more?
        return GetMoreSpecialized<CastExpression, VectorExpression>(c, l_cast,
                                                                    r_vector);
      }
    } break;

    default:
      LOG_ERROR(
          "This Peloton ExpressionType is in our map but not transformed here "
          ": %u",
          c);
  }

  // okay, still getTypedValue is beneficial.
  return GetGeneral(c, lc, rc);
}

// convert the enumerated value type into a concrete c type for the
// comparison helper templates.
AbstractExpression *ExpressionUtil::ComparisonFactory(PlannerDomValue obj,
                                                      ExpressionType et,
                                                      AbstractExpression *lc,
                                                      AbstractExpression *rc) {
  assert(lc);

  // more specialization available?
  ConstantValueExpression *l_const =
      dynamic_cast<ConstantValueExpression *>(lc);

  ConstantValueExpression *r_const =
      dynamic_cast<ConstantValueExpression *>(rc);

  TupleValueExpression *l_tuple = dynamic_cast<TupleValueExpression *>(lc);

  TupleValueExpression *r_tuple = dynamic_cast<TupleValueExpression *>(rc);

  // this will inline.GetValue(), hooray!
  if (l_const != NULL && r_const != NULL) {  // CONST-CONST can it happen?
    return GetMoreSpecialized<ConstantValueExpression, ConstantValueExpression>(
        et, l_const, r_const);
  } else if (l_const != NULL && r_tuple != NULL) {  // CONST-TUPLE
    return GetMoreSpecialized<ConstantValueExpression, TupleValueExpression>(
        et, l_const, r_tuple);
  } else if (l_tuple != NULL && r_const != NULL) {  // TUPLE-CONST
    return GetMoreSpecialized<TupleValueExpression, ConstantValueExpression>(
        et, l_tuple, r_const);
  } else if (l_tuple != NULL && r_tuple != NULL) {  // TUPLE-TUPLE
    return GetMoreSpecialized<TupleValueExpression, TupleValueExpression>(
        et, l_tuple, r_tuple);
  }

  SubqueryExpression *l_subquery = dynamic_cast<SubqueryExpression *>(lc);

  SubqueryExpression *r_subquery = dynamic_cast<SubqueryExpression *>(rc);

  if (l_subquery != NULL || r_subquery != NULL) {
    return SubqueryComparisonFactory(obj, et, lc, rc);
  }

  // okay, still.GetTypedValue.Is beneficial.
  return GetGeneral(et, lc, rc);
}

// convert the enumerated value type into a concrete c type for the
//  operator expression templated ctors
AbstractExpression *ExpressionUtil::OperatorFactory(ExpressionType et,
                                                    AbstractExpression *lc,
                                                    AbstractExpression *rc) {
  AbstractExpression *ret = NULL;

  switch (et) {
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
      ret = new OperatorExpression<OpPlus>(et, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MINUS):
      ret = new OperatorExpression<OpMinus>(et, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
      ret = new OperatorExpression<OpMultiply>(et, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
      ret = new OperatorExpression<OpDivide>(et, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_NOT):
      ret = new OperatorNotExpression(lc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_IS_NULL):
      ret = new OperatorIsNullExpression(lc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_EXISTS):
      ret = new OperatorExistsExpression(lc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MOD):
      throw Exception("Mod operator.Is not yet supported.");

    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
      throw Exception("Concat operator not yet supported.");

    default:
      throw Exception("operator ctor helper out of sync");
  }
  return ret;
}

// convert the enumerated value type into a concrete c type for the
//  operator expression templated ctors
AbstractExpression *ExpressionUtil::OperatorFactory(
    ExpressionType et, AbstractExpression *first, AbstractExpression *second,
    AbstractExpression *third, AbstractExpression *fourth) {
  AbstractExpression *ret = nullptr;

  switch (et) {
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
      ret = new OperatorExpression<OpPlus>(et, first, second);
      break;

    case EXPRESSION_TYPE_OPERATOR_UNARY_MINUS:
      ret = new OperatorUnaryMinusExpression(first);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MINUS):
      ret = new OperatorExpression<OpMinus>(et, first, second);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
      ret = new OperatorExpression<OpMultiply>(et, first, second);
      break;

    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
      ret = new OperatorExpression<OpDivide>(et, first, second);
      break;

    case (EXPRESSION_TYPE_OPERATOR_NOT):
      ret = new OperatorNotExpression(first);
      break;
    case (EXPRESSION_TYPE_SUBSTR):
      ret = new SubstringExpression(first, second, third);
      break;

    case (EXPRESSION_TYPE_CONCAT):
      ret = new ConcatExpression(first, second);
      break;

    case (EXPRESSION_TYPE_ASCII):
      ret = new AsciiExpression(first);
      break;

    case (EXPRESSION_TYPE_CHAR):
      ret = new CharExpression(first);
      break;

    case (EXPRESSION_TYPE_CHAR_LEN):
      ret = new CharLengthExpression(first);
      break;

    case (EXPRESSION_TYPE_OCTET_LEN):
      ret = new OctetLengthExpression(first);
      break;
    case (EXPRESSION_TYPE_POSITION):
      ret = new PositionExpression(first, second);
      break;
    case (EXPRESSION_TYPE_REPEAT):
      ret = new RepeatExpression(first, second);
      break;
    case (EXPRESSION_TYPE_LEFT):
      ret = new LeftExpression(first, second);
      break;
    case (EXPRESSION_TYPE_RIGHT):
      ret = new RightExpression(first, second);
      break;
    case (EXPRESSION_TYPE_REPLACE):
      ret = new ReplaceExpression(first, second, third);
      break;
    case (EXPRESSION_TYPE_OVERLAY):
      ret = new OverlayExpression(first, second, third, fourth);
      break;

    case (EXPRESSION_TYPE_LTRIM):
      ret = new LTrimExpression(first, second);
      break;
    case (EXPRESSION_TYPE_RTRIM):
      ret = new RTrimExpression(first, second);
      break;
    case (EXPRESSION_TYPE_BTRIM):
      ret = new BTrimExpression(first, second);
      break;
    case (EXPRESSION_TYPE_OPERATOR_MOD):
      ret = new OperatorExpression<OpMod>(et, first, second);
      break;
    case (EXPRESSION_TYPE_EXTRACT):
      ret = new ExtractExpression(first, second);
      break;
    case (EXPRESSION_TYPE_DATE_TO_TIMESTAMP):
      ret = new DateToTimestampExpression(first);
      break;
    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
      throw ExpressionException("Concat operator not yet supported.");

    case (EXPRESSION_TYPE_OPERATOR_CAST):
      throw ExpressionException("Cast operator not yet supported.");

    default:
      throw ExpressionException("operator ctor helper out of sync");
  }
  return ret;
}

AbstractExpression *ExpressionUtil::CastFactory(ValueType vt,
                                                AbstractExpression *lc) {
  return new OperatorCastExpression(vt, lc);
}

AbstractExpression *ExpressionUtil::CastFactory(PostgresValueType type,
                                                AbstractExpression *child) {
  return new expression::CastExpression(type, child);
}

// provide an interface for creating constant value expressions that
// is more useful to testcases
AbstractExpression *ExpressionUtil::ConstantValueFactory(
    const Value &newvalue) {
  return new ConstantValueExpression(newvalue);
}

// convert the enumerated value type into a concrete type for
// constant value expression templated ctors
AbstractExpression *ExpressionUtil::ConstantValueFactory(
    PlannerDomValue obj, ValueType vt,
    __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read before ctor - can then instantiate fully init'd obj.
  Value newvalue;
  bool IsNull = obj.valueForKey("ISNULL").asBool();
  if (IsNull) {
    newvalue = Value::GetNullValue(vt);
    return new ConstantValueExpression(newvalue);
  }

  PlannerDomValue valueValue = obj.valueForKey("VALUE");

  switch (vt) {
    case VALUE_TYPE_INVALID:
      throw Exception(
          "constantValueFactory: Value type should"
          " never be VALUE_TYPE_INVALID");
    case VALUE_TYPE_NULL:
      throw Exception(
          "constantValueFactory: And they should be"
          " never be this either! VALUE_TYPE_NULL");
    case VALUE_TYPE_TINYINT:
      newvalue = ValueFactory::GetTinyIntValue(
          static_cast<int8_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_SMALLINT:
      newvalue = ValueFactory::GetSmallIntValue(
          static_cast<int16_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_INTEGER:
      newvalue = ValueFactory::GetIntegerValue(
          static_cast<int32_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_BIGINT:
      newvalue = ValueFactory::GetBigIntValue(
          static_cast<int64_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_DOUBLE:
      newvalue = ValueFactory::GetDoubleValue(
          static_cast<double>(valueValue.asDouble()));
      break;
    case VALUE_TYPE_VARCHAR:
      newvalue = ValueFactory::GetStringValue(valueValue.asStr());
      break;
    case VALUE_TYPE_VARBINARY:
      // uses hex encoding
      newvalue = ValueFactory::GetBinaryValue(valueValue.asStr());
      break;
    case VALUE_TYPE_TIMESTAMP:
      newvalue = ValueFactory::GetTimestampValue(
          static_cast<int64_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_DECIMAL:
      newvalue = ValueFactory::GetDecimalValueFromString(valueValue.asStr());
      break;
    case VALUE_TYPE_BOOLEAN:
      newvalue = ValueFactory::GetBooleanValue(valueValue.asBool());
      break;
    default:
      throw Exception(
          "constantValueFactory: Unrecognized value"
          " type");
  }

  return new ConstantValueExpression(newvalue);
}

// convert the enumerated value type into a concrete c type for
// constant value expressions templated ctors
AbstractExpression *ExpressionUtil::ConstantValueFactory(
    json_spirit::Object &obj, __attribute__((unused)) ValueType vt,
    __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read before ctor - can then instantiate fully init'd obj.
  Value newvalue;
  json_spirit::Value valueValue = json_spirit::find_value(obj, "VALUE");
  if (valueValue == json_spirit::Value::null) {
    throw ExpressionException(
        "constantValueFactory: Could not find"
        " VALUE value");
  }

  if (valueValue.type() == json_spirit::str_type) {
    std::string nullcheck = valueValue.get_str();
    if (nullcheck == "nullptr") {
      newvalue = Value::GetNullValue(vt);
      return ConstantValueFactory(newvalue);
    }
  }

  switch (vt) {
    case VALUE_TYPE_INVALID:
      throw ExpressionException(
          "constantValueFactory: Value type should"
          " never be VALUE_TYPE_INVALID");
    case VALUE_TYPE_NULL:
      throw ExpressionException(
          "constantValueFactory: And they should be"
          " never be this either! VALUE_TYPE_nullptr");
    case VALUE_TYPE_TINYINT:
      newvalue = ValueFactory::GetTinyIntValue(
          static_cast<int8_t>(valueValue.get_int64()));
      break;
    case VALUE_TYPE_SMALLINT:
      newvalue = ValueFactory::GetSmallIntValue(
          static_cast<int16_t>(valueValue.get_int64()));
      break;
    case VALUE_TYPE_INTEGER:
      newvalue = ValueFactory::GetIntegerValue(
          static_cast<int32_t>(valueValue.get_int64()));
      break;
    case VALUE_TYPE_BIGINT:
      newvalue = ValueFactory::GetBigIntValue(
          static_cast<int64_t>(valueValue.get_int64()));
      break;
    case VALUE_TYPE_DOUBLE:
      newvalue = ValueFactory::GetDoubleValue(
          static_cast<double>(valueValue.get_real()));
      break;
    case VALUE_TYPE_VARCHAR:
      newvalue = ValueFactory::GetStringValue(valueValue.get_str());
      break;
    case VALUE_TYPE_VARBINARY:
      // uses hex encoding
      newvalue = ValueFactory::GetBinaryValue(valueValue.get_str());
      break;
    case VALUE_TYPE_TIMESTAMP:
      newvalue = ValueFactory::GetTimestampValue(
          static_cast<int64_t>(valueValue.get_int64()));
      break;
    case VALUE_TYPE_DECIMAL:
      newvalue = ValueFactory::GetDecimalValueFromString(valueValue.get_str());
      break;
    default:
      throw ExpressionException(
          "constantValueFactory: Unrecognized value"
          " type");
  }

  return ConstantValueFactory(newvalue);
}

AbstractExpression *ExpressionUtil::VectorFactory(
    ValueType elementType, const std::vector<AbstractExpression *> &arguments) {
  return new VectorExpression(elementType, arguments);
}

AbstractExpression *ExpressionUtil::ParameterValueFactory(int idx) {
  return new ParameterValueExpression(idx);
}

// convert the enumerated value type into a concrete c type for
// parameter value expression templated ctors
AbstractExpression *ExpressionUtil::ParameterValueFactory(
    PlannerDomValue obj, __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read before ctor - can then instantiate fully init'd obj.
  int param_idx = obj.valueForKey("PARAM_IDX").asInt();
  assert(param_idx >= 0);
  return new ParameterValueExpression(param_idx);
}

// convert the enumerated value type into a concrete c type for
// parameter value expression templated ctors */
AbstractExpression *ExpressionUtil::ParameterValueFactory(
    json_spirit::Object &obj, __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read before ctor - can then instantiate fully init'd obj.
  json_spirit::Value paramIdxValue = json_spirit::find_value(obj, "PARAM_IDX");
  if (paramIdxValue == json_spirit::Value::null) {
    throw ExpressionException(
        "parameterValueFactory: Could not find"
        " PARAM_IDX value");
  }

  int param_idx = paramIdxValue.get_int();
  assert(param_idx >= 0);
  return ParameterValueFactory(param_idx);
}

AbstractExpression *ExpressionUtil::TupleValueFactory(int tuple_idx,
                                                      int value_idx) {
  return new TupleValueExpression(tuple_idx, value_idx);
}

// convert the enumerated value type into a concrete c type for
// tuple value expression templated ctors
AbstractExpression *ExpressionUtil::TupleValueFactory(
    PlannerDomValue obj, __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read the tuple value expression specific data
  int columnIndex = obj.valueForKey("COLUMN_IDX").asInt();
  int tableIdx = 0;
  if (obj.hasNonNullKey("TABLE_IDX")) {
    tableIdx = obj.valueForKey("TABLE_IDX").asInt();
  }

  // verify input
  if (columnIndex < 0) {
    std::ostringstream message;
    message << "tupleValueFactory: invalid column_idx " << columnIndex
            << " for " << ((tableIdx == 0) ? "" : "inner ");
    // comment out because the exception type not found.
    throw Exception(message.str());
  }

  return new TupleValueExpression(tableIdx, columnIndex);
}

// convert the enumerated value type into a concrete c type for
// tuple value expression templated ctors
AbstractExpression *ExpressionUtil::TupleValueFactory(
    json_spirit::Object &obj, __attribute__((unused)) ExpressionType et,
    __attribute__((unused)) AbstractExpression *lc,
    __attribute__((unused)) AbstractExpression *rc) {
  // read the tuple value expression specific data
  json_spirit::Value valueIdxValue = json_spirit::find_value(obj, "COLUMN_IDX");

  json_spirit::Value tableName = json_spirit::find_value(obj, "TABLE_NAME");

  json_spirit::Value columnName = json_spirit::find_value(obj, "COLUMN_NAME");

  // verify input
  if (valueIdxValue == json_spirit::Value::null) {
    throw ExpressionException(
        "tupleValueFactory: Could not find"
        " COLUMN_IDX value");
  }
  if (valueIdxValue.get_int() < 0) {
    throw ExpressionException("tupleValueFactory: invalid column_idx.");
  }

  if (tableName == json_spirit::Value::null) {
    throw ExpressionException("tupleValueFactory: no table name in TVE");
  }

  if (columnName == json_spirit::Value::null) {
    throw ExpressionException(
        "tupleValueFactory: no column name in"
        " TVE");
  }

  // Hard-coded as the left tuple
  int tuple_idx = 0;
  return new TupleValueExpression(tuple_idx, valueIdxValue.get_int());
}

AbstractExpression *ExpressionUtil::ConjunctionFactory(ExpressionType et,
                                                       AbstractExpression *lc,
                                                       AbstractExpression *rc) {
  switch (et) {
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
      return new ConjunctionExpression<ConjunctionAnd>(et, lc, rc);
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
      return new ConjunctionExpression<ConjunctionOr>(et, lc, rc);
    default:
      return NULL;
  }
}

//
// @brief Construct a conjunction expression from a list of AND'ed or OR'ed
// expressions
// @return A constructed peloton expression
AbstractExpression *ExpressionUtil::ConjunctionFactory(
    ExpressionType et, std::list<AbstractExpression *> exprs) {
  if (exprs.empty())
    return expression::ExpressionUtil::ConstantValueFactory(
        ValueFactory::GetBooleanValue(true));

  AbstractExpression *front = exprs.front();
  exprs.pop_front();

  if (exprs.empty()) return front;
  switch (et) {
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
      return new expression::ConjunctionExpression<ConjunctionAnd>(
          et, front, expression::ExpressionUtil::ConjunctionFactory(et, exprs));
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
      return new expression::ConjunctionExpression<ConjunctionOr>(
          et, front, expression::ExpressionUtil::ConjunctionFactory(et, exprs));
    default:
      return nullptr;
  }
}

void RaiseFunctionFactoryError(const std::string &nameString, int functionId,
                               const std::vector<AbstractExpression *> &args) {
  char fn_message[1024];
  snprintf(fn_message, sizeof(fn_message),
           "Internal Error: SQL function '%s' with ID (%d) with (%d) "
           "parameters.Is not implemented in VoltDB (or may have been "
           "incorrectly parsed)",
           nameString.c_str(), functionId, (int)args.size());
  // DEBUG_ASSERT_OR_THROW_OR_CRASH(false, fn_message);
  throw Exception(fn_message);
}

// Given an expression type and a valuetype, find the best
// templated ctor to invoke. Several helpers, above, aid in this
// pursuit. Each instantiated expression must consume any
// class-specific serialization from serialize_io. */
AbstractExpression *ExpressionUtil::ExpressionFactory(
    PlannerDomValue obj, ExpressionType et, ValueType vt, int vs,
    AbstractExpression *lc, AbstractExpression *rc,
    const std::vector<AbstractExpression *> &args) {
  AbstractExpression *ret = NULL;

  switch (et) {
    // Casts
    case (EXPRESSION_TYPE_OPERATOR_CAST):
      ret = CastFactory(vt, lc);
      break;

    // Operators
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
    case (EXPRESSION_TYPE_OPERATOR_MINUS):
    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
    case (EXPRESSION_TYPE_OPERATOR_MOD):
    case (EXPRESSION_TYPE_OPERATOR_NOT):
    case (EXPRESSION_TYPE_OPERATOR_IS_NULL):
    case (EXPRESSION_TYPE_OPERATOR_EXISTS):
      ret = ExpressionUtil::OperatorFactory(et, lc, rc);
      break;

    // Comparisons
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
    case (EXPRESSION_TYPE_COMPARE_NOTLIKE):
    case (EXPRESSION_TYPE_COMPARE_IN):
      ret = ComparisonFactory(obj, et, lc, rc);
      break;

    // Conjunctions
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
      ret = ExpressionUtil::ConjunctionFactory(et, lc, rc);
      break;

    // Functions and pseudo-functions
    case (EXPRESSION_TYPE_FUNCTION): {
      // add the function id
      int functionId = obj.valueForKey("FUNCTION_ID").asInt();
      ret = FunctionFactory(functionId, args);

      if (!ret) {
        std::string nameString;
        if (obj.hasNonNullKey("NAME")) {
          nameString = obj.valueForKey("NAME").asStr();
        } else {
          nameString = "?";
        }
        RaiseFunctionFactoryError(nameString, functionId, args);
      }
    } break;

    case (EXPRESSION_TYPE_VALUE_VECTOR): {
      // Parse whatever.Is needed out of obj and pass the pieces to
      // inListFactory
      // to make it easier to unit test independently of the parsing.
      // The first argumenthis used as the list element type.
      // If the ValueType of the .Ist builder expression needs to be "ARRAY" or
      // something else,
      // a separate element type attribute will have to be serialized and passed
      // in here.
      ret = VectorFactory(vt, args);
    } break;

    // Constant Values, parameters, tuples
    case (EXPRESSION_TYPE_VALUE_CONSTANT):
      ret = ConstantValueFactory(obj, vt, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_PARAMETER):
      ret = ParameterValueFactory(obj, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_TUPLE):
      ret = TupleValueFactory(obj, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
      ret = new TupleAddressExpression();
      break;
    case (EXPRESSION_TYPE_VALUE_SCALAR):
      ret = new ScalarValueExpression(lc);
      break;
    case (EXPRESSION_TYPE_HASH_RANGE):
      ret = HashRangeFactory(obj);
      break;
    // Anyway, this function is not implemented. comment out.
    //    case (EXPRESSION_TYPE_OPERATOR_ALTERNATIVE):
    //      ret = new OperatorAlternativeExpression(lc, rc);
    //      break;

    // Subquery
    case (EXPRESSION_TYPE_ROW_SUBQUERY):
    case (EXPRESSION_TYPE_SELECT_SUBQUERY):
      ret = SubqueryFactory(et, obj, args);
      break;

    // must handle all known expression in this factory
    default:

      char message[256];
      snprintf(message, 256,
               "Invalid ExpressionType '%s' (%d) requested from factory",
               ExpressionTypeToString(et).c_str(), (int)et);
      throw Exception(message);
  }

  ret->setValueType(vt);
  ret->setValueSize(vs);
  // written thusly to ease testing/inspecting return content.
  LOG_TRACE("Created expression %p", ret);
  return ret;
}

// Given an expression type and a valuetype, find the best
// templated ctor to invoke. Several helpers, above, aid in this
// pursuit. Each instantiated expression must consume any
// class-specific serialization from serialize_io.
AbstractExpression *ExpressionUtil::ExpressionFactory(
    json_spirit::Object &obj, ExpressionType et, ValueType vt,
    __attribute__((unused)) int vs, AbstractExpression *lc,
    AbstractExpression *rc) {
  LOG_TRACE("expressionFactory request: ");
  LOG_TRACE("%s %d", ExpressionTypeToString(et).c_str(), et);
  LOG_TRACE("%d %d", vt, vs);

  AbstractExpression *ret = nullptr;

  switch (et) {
    // Operators
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
    case (EXPRESSION_TYPE_OPERATOR_MINUS):
    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
    case (EXPRESSION_TYPE_OPERATOR_MOD):
    case (EXPRESSION_TYPE_OPERATOR_CAST):
    case (EXPRESSION_TYPE_OPERATOR_NOT):
      ret = OperatorFactory(et, lc, rc);
      break;

    // Comparisons
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
    case (EXPRESSION_TYPE_COMPARE_NOTLIKE):
      ret = ComparisonFactory(et, lc, rc);
      break;

    // Conjunctions
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
      ret = ConjunctionFactory(et, lc, rc);
      break;

    // Constant Values, parameters, tuples
    case (EXPRESSION_TYPE_VALUE_CONSTANT):
      ret = ConstantValueFactory(obj, vt, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_PARAMETER):
      ret = ParameterValueFactory(obj, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_TUPLE):
      ret = TupleValueFactory(obj, et, lc, rc);
      break;

    case (EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
      ret = new TupleAddressExpression();
      break;

    // must handle all known expressions in this factory
    default:
      char message[256];
      sprintf(message, "Invalid ExpressionType '%s' requested from factory",
              ExpressionTypeToString(et).c_str());
      throw ExpressionException(message);
  }

  // written thusly to ease testing/inspecting return content.
  LOG_TRACE("Created %s", ExpressionTypeToString(et).c_str());
  return ret;
}

boost::shared_array<int> ExpressionUtil::ConvertIfAllTupleValues(
    const std::vector<AbstractExpression *> &expression) {
  size_t cnt = expression.size();
  boost::shared_array<int> ret(new int[cnt]);
  for (size_t i = 0; i < cnt; ++i) {
    TupleValueExpression *casted =
        dynamic_cast<TupleValueExpression *>(expression[i]);
    if (casted == NULL) {
      return boost::shared_array<int>();
    }
    ret[i] = casted->GetColumnId();
  }
  return ret;
}

boost::shared_array<int> ExpressionUtil::ConvertIfAllParameterValues(
    const std::vector<AbstractExpression *> &expression) {
  size_t cnt = expression.size();
  boost::shared_array<int> ret(new int[cnt]);
  for (size_t i = 0; i < cnt; ++i) {
    ParameterValueExpression *casted =
        dynamic_cast<ParameterValueExpression *>(expression[i]);
    if (casted == NULL) {
      return boost::shared_array<int>();
    }
    ret[i] = casted->GetParameterId();
  }
  return ret;
}

void ExpressionUtil::ExtractTupleValuesColumnIdx(const AbstractExpression *expr,
                                                 std::vector<int> &columnIds) {
  if (expr == NULL) {
    return;
  }
  if (expr->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
    const TupleValueExpression *tve =
        dynamic_cast<const TupleValueExpression *>(expr);
    assert(tve != NULL);
    columnIds.push_back(tve->GetColumnId());
    return;
  }
  // Recurse
  ExpressionUtil::ExtractTupleValuesColumnIdx(expr->GetLeft(), columnIds);
  ExpressionUtil::ExtractTupleValuesColumnIdx(expr->GetRight(), columnIds);
}

}  // End expression namespace
}  // End peloton namespace
