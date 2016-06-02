//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// expression_util.cpp
//
// Identification: src/expression/expression_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <cstdlib>
#include <stdexcept>

#include "common/value_factory.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "expression/expression_util.h"
#include "expression/abstract_expression.h"
#include "expression/hash_range_expression.h"
#include "expression/operator_expression.h"
#include "expression/comparison_expression.h"
#include "expression/case_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/tuple_value_expression.h"
#include "expression/cast_expression.h"
#include "expression/tuple_address_expression.h"
#include "expression/scalar_value_expression.h"
#include "expression/vector_expression.h"
#include "expression/function_expression.h"
#include "expression/subquery_expression.h"
#include "expression/string_expression.h"
#include "expression/date_expression.h"
#include "expression/vector_comparison_expression.h"
#include "expression/coalesce_expression.h"
#include "expression/nullif_expression.h"
#include "expression/udf_expression.h"

namespace peloton {
namespace expression {

AbstractExpression *GetGeneral(ExpressionType c, AbstractExpression *l,
                               AbstractExpression *r) {
  PL_ASSERT(l);
  PL_ASSERT(r);
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
  PL_ASSERT(l);
  PL_ASSERT(r);
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
  PL_ASSERT(lc);

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
//  operator expression templated ctors
AbstractExpression *ExpressionUtil::OperatorFactory(ExpressionType et,
                                                    ValueType vt,
                                                    AbstractExpression *lc,
                                                    AbstractExpression *rc) {
  AbstractExpression *ret = NULL;

  switch (et) {
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
      ret = new OperatorExpression<OpPlus>(et, vt, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MINUS):
      ret = new OperatorExpression<OpMinus>(et, vt, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
      ret = new OperatorExpression<OpMultiply>(et, vt, lc, rc);
      break;

    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
      ret = new OperatorExpression<OpDivide>(et, vt, lc, rc);
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
    ExpressionType et, ValueType vt, AbstractExpression *first,
    AbstractExpression *second, AbstractExpression *third,
    AbstractExpression *fourth) {
  AbstractExpression *ret = nullptr;

  switch (et) {
    case (EXPRESSION_TYPE_OPERATOR_PLUS):
      ret = new OperatorExpression<OpPlus>(et, vt, first, second);
      break;

    case EXPRESSION_TYPE_OPERATOR_UNARY_MINUS:
      ret = new OperatorUnaryMinusExpression(first);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MINUS):
      ret = new OperatorExpression<OpMinus>(et, vt, first, second);
      break;

    case (EXPRESSION_TYPE_OPERATOR_MULTIPLY):
      ret = new OperatorExpression<OpMultiply>(et, vt, first, second);
      break;

    case (EXPRESSION_TYPE_OPERATOR_DIVIDE):
      ret = new OperatorExpression<OpDivide>(et, vt, first, second);
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
      ret = new OperatorExpression<OpMod>(et, vt, first, second);
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
  return new CastExpression(vt, lc);
}

// provide an interface for creating constant value expressions that
// is more useful to testcases
AbstractExpression *ExpressionUtil::ConstantValueFactory(
    const Value &newvalue) {
  return new ConstantValueExpression(newvalue);
}

AbstractExpression *ExpressionUtil::VectorFactory(
    ValueType elementType, const std::vector<AbstractExpression *> &arguments) {
  return new VectorExpression(elementType, arguments);
}

AbstractExpression *ExpressionUtil::ParameterValueFactory(ValueType type,
                                                          int idx) {
  return new ParameterValueExpression(type, idx);
}

AbstractExpression *ExpressionUtil::TupleValueFactory(ValueType type,
                                                      int tuple_idx,
                                                      int value_idx) {
  return new TupleValueExpression(type, tuple_idx, value_idx);
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
      return nullptr;
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

AbstractExpression *ExpressionUtil::UDFExpressionFactory(
    Oid function_id, Oid collation, Oid return_type,
    std::vector<std::unique_ptr<expression::AbstractExpression>> &args) {
  return new expression::UDFExpression(function_id, collation, return_type,
                                       args);
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

boost::shared_array<int> ExpressionUtil::ConvertIfAllTupleValues(
    const std::vector<AbstractExpression *> &expression) {
  size_t cnt = expression.size();
  boost::shared_array<int> ret(new int[cnt]);
  for (size_t i = 0; i < cnt; ++i) {
    TupleValueExpression *casted =
        dynamic_cast<TupleValueExpression *>(expression[i]);
    if (casted == nullptr) {
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
  if (expr == nullptr) {
    return;
  }
  if (expr->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE) {
    const TupleValueExpression *tve =
        dynamic_cast<const TupleValueExpression *>(expr);
    PL_ASSERT(tve != NULL);
    columnIds.push_back(tve->GetColumnId());
    return;
  }
  // Recurse
  ExpressionUtil::ExtractTupleValuesColumnIdx(expr->GetLeft(), columnIds);
  ExpressionUtil::ExtractTupleValuesColumnIdx(expr->GetRight(), columnIds);
}

}  // End expression namespace
}  // End peloton namespace
