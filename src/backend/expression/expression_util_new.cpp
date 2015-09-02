//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// expression_util.cpp
//
// Identification: src/backend/expression/expression_util.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/logger.h"
#include "backend/common/value_factory.h"
#include "backend/common/exception.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/abstract_expression.h"
#include "backend/expression/hash_range_expression.h"
#include "backend/expression/expressions.h"
#include "backend/common/types.h"

#include <cassert>
#include <sstream>
#include <cstdlib>
#include <stdexcept>

namespace peloton {
namespace expression {

/** Parse JSON parameters to create a hash range expression */
static AbstractExpression*
hashRangeFactory(PlannerDomValue obj) {
  PlannerDomValue hashColumnValue = obj.valueForKey("HASH_COLUMN");

  PlannerDomValue rangesArray = obj.valueForKey("RANGES");

  srange_type *ranges = new srange_type[rangesArray.arrayLen()];
  for (int ii = 0; ii < rangesArray.arrayLen(); ii++) {
    PlannerDomValue arrayObject = rangesArray.valueAtIndex(ii);
    PlannerDomValue rangeStartValue = arrayObject.valueForKey("RANGE_START");
    PlannerDomValue rangeEndValue = arrayObject.valueForKey("RANGE_END");

    ranges[ii] = srange_type(rangeStartValue.asInt(), rangeEndValue.asInt());
  }
  return new HashRangeExpression(hashColumnValue.asInt(), ranges, static_cast<int>(rangesArray.arrayLen()));
}

/** Parse JSON parameters to create a subquery expression */
static AbstractExpression*
subqueryFactory(ExpressionType subqueryType, PlannerDomValue obj, const std::vector<AbstractExpression*>* args) {
  int subqueryId = obj.valueForKey("SUBQUERY_ID").asInt();
  std::vector<int> paramIdxs;
  if (obj.hasNonNullKey("PARAM_IDX")) {
    PlannerDomValue params = obj.valueForKey("PARAM_IDX");
    int paramSize = params.arrayLen();
    paramIdxs.reserve(paramSize);
    if (args == NULL || args->size() != paramSize) {
      throw Exception(
                                    "subqueryFactory: parameter indexes/tve count .Ismatch");
    }
    for (int i = 0; i < paramSize; ++i) {
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
  return new SubqueryExpression(subqueryType, subqueryId, paramIdxs, otherParamIdxs, args);
}

/** Function static helper templated functions to vivify an optimal
    comparison class. */
static AbstractExpression*
subqueryComparisonFactory(PlannerDomValue obj,
                          ExpressionType c,
                          AbstractExpression *l,
                          AbstractExpression *r)
{
  QuantifierType quantifier = QUANTIFIER_TYPE_NONE;
  if (obj.hasNonNullKey("QUANTIFIER")) {
    quantifier = static_cast<QuantifierType>(obj.valueForKey("QUANTIFIER").asInt());
  }

  SubqueryExpression *l_subquery =
      dynamic_cast<SubqueryExpression*>(l);

  SubqueryExpression *r_subquery =
      dynamic_cast<SubqueryExpression*>(r);

  // OK, here we go
  if (l_subquery != NULL && r_subquery != NULL) {
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
                return new VectorComparisonExpression<CmpEq, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
                return new VectorComparisonExpression<CmpNe, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
                return new VectorComparisonExpression<CmpLt, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
                return new VectorComparisonExpression<CmpGt, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
                return new VectorComparisonExpression<CmpLte, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
                return new VectorComparisonExpression<CmpGte, TupleExtractor, TupleExtractor>(c, l, r, quantifier);
      default:
        char message[256];
        snprintf(message, 256, "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception( message);
    }
  } else if (l_subquery != NULL) {
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
                return new VectorComparisonExpression<CmpEq, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
                return new VectorComparisonExpression<CmpNe, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
                return new VectorComparisonExpression<CmpLt, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
                return new VectorComparisonExpression<CmpGt, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
                return new VectorComparisonExpression<CmpLte, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
                return new VectorComparisonExpression<CmpGte, TupleExtractor, ValueExtractor>(c, l, r, quantifier);
      default:
        char message[256];
        snprintf(message, 256, "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception( message);
    }
  } else {
    assert(r_subquery != NULL);
    switch (c) {
      case (EXPRESSION_TYPE_COMPARE_EQUAL):
                return new VectorComparisonExpression<CmpEq, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
                return new VectorComparisonExpression<CmpNe, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
                return new VectorComparisonExpression<CmpLt, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
                return new VectorComparisonExpression<CmpGt, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
                return new VectorComparisonExpression<CmpLte, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
                return new VectorComparisonExpression<CmpGte, ValueExtractor, TupleExtractor>(c, l, r, quantifier);
      default:
        char message[256];
        snprintf(message, 256, "Invalid ExpressionType '%s' called"
                 " for VectorComparisonExpression",
                 ExpressionTypeToString(c).c_str());
        throw Exception( message);
    }
  }
}

static AbstractExpression*
getGeneral(ExpressionType c,
           AbstractExpression *l,
           AbstractExpression *r)
{
  assert (l);
  assert (r);
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
    case (EXPRESSION_TYPE_COMPARE_IN):
            return new ComparisonExpression<CmpIn>(c, l, r);
    default:
      char message[256];
      snprintf(message, 256, "Invalid ExpressionType '%s' called"
               " for ComparisonExpression",
               ExpressionTypeToString(c).c_str());
      throw Exception( message);
  }
}


template <typename L, typename R>
static AbstractExpression*
getMoreSpecialized(ExpressionType c, L* l, R* r)
{
  assert (l);
  assert (r);
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
    case (EXPRESSION_TYPE_COMPARE_IN):
            return new InlinedComparisonExpression<CmpIn, L, R>(c, l, r);
    default:
      char message[256];
      snprintf(message, 256, "Invalid ExpressionType '%s' called for"
               " ComparisonExpression",ExpressionTypeToString(c).c_str());
      throw Exception( message);
  }
}

/** convert the enumerated value type into a concrete c type for the
 * comparison helper templates. */
AbstractExpression *
ExpressionUtil::ComparisonFactory(PlannerDomValue obj, ExpressionType et, AbstractExpression *lc, AbstractExpression *rc)
{
  assert(lc);

  // more specialization available?
  ConstantValueExpression *l_const =
      dynamic_cast<ConstantValueExpression*>(lc);

  ConstantValueExpression *r_const =
      dynamic_cast<ConstantValueExpression*>(rc);

  TupleValueExpression *l_tuple =
      dynamic_cast<TupleValueExpression*>(lc);

  TupleValueExpression *r_tuple =
      dynamic_cast<TupleValueExpression*>(rc);

  // this will inline.GetValue(), hooray!
  if (l_const != NULL && r_const != NULL) { // CONST-CONST can it happen?
    return getMoreSpecialized<ConstantValueExpression, ConstantValueExpression>(et, l_const, r_const);
  } else if (l_const != NULL && r_tuple != NULL) { // CONST-TUPLE
    return getMoreSpecialized<ConstantValueExpression, TupleValueExpression>(et, l_const, r_tuple);
  } else if (l_tuple != NULL && r_const != NULL) { // TUPLE-CONST
    return getMoreSpecialized<TupleValueExpression, ConstantValueExpression >(et, l_tuple, r_const);
  } else if (l_tuple != NULL && r_tuple != NULL) { // TUPLE-TUPLE
    return getMoreSpecialized<TupleValueExpression, TupleValueExpression>(et, l_tuple, r_tuple);
  }

  SubqueryExpression *l_subquery =
      dynamic_cast<SubqueryExpression*>(lc);

  SubqueryExpression *r_subquery =
      dynamic_cast<SubqueryExpression*>(rc);

  if (l_subquery != NULL || r_subquery != NULL) {
    return subqueryComparisonFactory(obj, et, lc, rc);
  }

  //okay, still.GetTypedValue.Is beneficial.
  return getGeneral(et, lc, rc);
}

/** convert the enumerated value type into a concrete c type for the
 *  operator expression templated ctors */
static AbstractExpression *
operatorFactory(ExpressionType et,
                AbstractExpression *lc, AbstractExpression *rc)
{
  AbstractExpression *ret = NULL;

  switch(et) {
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
           throw Exception(
                                         "Mod operator.Is not yet supported.");

    case (EXPRESSION_TYPE_OPERATOR_CONCAT):
           throw Exception(
                                         "Concat operator not yet supported.");

    default:
      throw Exception(
                                    "operator ctor helper out of sync");
  }
  return ret;
}

static AbstractExpression* castFactory(ValueType vt,
                                       AbstractExpression *lc)
{
  return new OperatorCastExpression(vt, lc);
}

static AbstractExpression* caseWhenFactory(ValueType vt,
                                           AbstractExpression *lc, AbstractExpression *rc)
{

  OperatorAlternativeExpression* alternative = dynamic_cast<OperatorAlternativeExpression*> (rc);
  if (!rc) {
    throw Exception(
                                  "operator case when has incorrect expression");
  }
  return new OperatorCaseWhenExpression(vt, lc, alternative);
}


/** convert the enumerated value type into a concrete type for
 * constant value expression templated ctors */
static AbstractExpression*
constantValueFactory(PlannerDomValue obj,
                     ValueType vt, ExpressionType et,
                     AbstractExpression *lc, AbstractExpression *rc)
{
  // read before ctor - can then instantiate fully init'd obj.
  Value newvalue;
  bool.IsNull = obj.valueForKey("ISNULL").asBool();
  if .IsNull)
  {
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
      newvalue = ValueFactory::GetTinyIntValue(static_cast<int8_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_SMALLINT:
      newvalue = ValueFactory::GetSmallIntValue(static_cast<int16_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_INTEGER:
      newvalue = ValueFactory::GetIntegerValue(static_cast<int32_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_BIGINT:
      newvalue = ValueFactory::GetBigIntValue(static_cast<int64_t>(valueValue.asInt64()));
      break;
    case VALUE_TYPE_DOUBLE:
      newvalue = ValueFactory::GetDoubleValue(static_cast<double>(valueValue.asDouble()));
      break;
    case VALUE_TYPE_VARCHAR:
      newvalue = ValueFactory::GetStringValue(valueValue.asStr());
      break;
    case VALUE_TYPE_VARBINARY:
      // uses hex encoding
      newvalue = ValueFactory::GetBinaryValue(valueValue.asStr());
      break;
    case VALUE_TYPE_TIMESTAMP:
      newvalue = ValueFactory::GetTimestampValue(static_cast<int64_t>(valueValue.asInt64()));
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


/** convert the enumerated value type into a concrete c type for
 * parameter value expression templated ctors */
static AbstractExpression*
parameterValueFactory(PlannerDomValue obj,
                      ExpressionType et,
                      AbstractExpression *lc, AbstractExpression *rc)
{
  // read before ctor - can then instantiate fully init'd obj.
  int param_idx = obj.valueForKey("PARAM_IDX").asInt();
  assert (param_idx >= 0);
  return new ParameterValueExpression(param_idx);
}

/** convert the enumerated value type into a concrete c type for
 * tuple value expression templated ctors */
static AbstractExpression*
tupleValueFactory(PlannerDomValue obj, ExpressionType et,
                  AbstractExpression *lc, AbstractExpression *rc)
{
  // read the tuple value expression specific data
  int columnIndex = obj.valueForKey("COLUMN_IDX").asInt();
  int tableIdx = 0;
  if (obj.hasNonNullKey("TABLE_IDX")) {
    tableIdx = obj.valueForKey("TABLE_IDX").asInt();
  }

  // verify input
  if (columnIndex < 0) {
    std::ostringstream message;
    message << "tupleValueFactory: invalid column_idx " << columnIndex <<
        " for " << ((tableIdx == 0) ? "" : "inner ") << "table\nStack trace:\n" <<
        StackTrace::stringStackTrace();
    throw UnexpectedEEException(message.str());
  }

  return new TupleValueExpression(tableIdx, columnIndex);
}


AbstractExpression *
ExpressionUtil::ConjunctionFactory(ExpressionType et, AbstractExpression *lc, AbstractExpression *rc)
{
  switch (et) {
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
            return new ConjunctionExpression<ConjunctionAnd>(et, lc, rc);
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
            return new ConjunctionExpression<ConjunctionOr>(et, lc, rc);
    default:
      return NULL;
  }

}

static void raiseFunctionFactoryError(const std::string& nameString, int functionId,
                                      const std::vector<AbstractExpression*>* args)
{
  char fn_message[1024];
  if (args) {
    snprintf(fn_message, sizeof(fn_message),
             "Internal Error: SQL function '%s' with ID (%d) with (%d) parameters.Is not implemented in VoltDB (or may have been incorrectly parsed)",
             nameString.c_str(), functionId, (int)args->size());
  }
  else {
    snprintf(fn_message, sizeof(fn_message),
             "Internal Error: SQL function '%s' with ID (%d) was serialized without its required parameters .Ist.",
             nameString.c_str(), functionId);
  }
  //DEBUG_ASSERT_OR_THROW_OR_CRASH(false, fn_message);
  throw Exception( fn_message);
}


/** Given an expression type and a valuetype, find the best
 * templated ctor to invoke. Several helpers, above, aid in this
 * pursuit. Each instantiated expression must consume any
 * class-specific serialization from serialize_io. */
AbstractExpression*
ExpressionUtil::expressionFactory(PlannerDomValue obj,
                                  ExpressionType et, ValueType vt, int vs,
                                  AbstractExpression* lc,
                                  AbstractExpression* rc,
                                  const std::vector<AbstractExpression*>* args)
{
  AbstractExpression *ret = NULL;

  switch (et) {

    // Casts
    case (EXPRESSION_TYPE_OPERATOR_CAST):
            ret = castFactory(vt, lc);
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
    ret = operatorFactory(et, lc, rc);
    break;

    // Comparisons
    case (EXPRESSION_TYPE_COMPARE_EQUAL):
    case (EXPRESSION_TYPE_COMPARE_NOTEQUAL):
    case (EXPRESSION_TYPE_COMPARE_LESSTHAN):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHAN):
    case (EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO):
    case (EXPRESSION_TYPE_COMPARE_LIKE):
    case (EXPRESSION_TYPE_COMPARE_IN):
    ret = ComparisonFactory(obj, et, lc, rc);
    break;

    // Conjunctions
    case (EXPRESSION_TYPE_CONJUNCTION_AND):
    case (EXPRESSION_TYPE_CONJUNCTION_OR):
    ret = ConjunctionFactory(et, lc, rc);
    break;

    // Functions and pseudo-functions
    case (EXPRESSION_TYPE_FUNCTION): {
      // add the function id
      int functionId = obj.valueForKey("FUNCTION_ID").asInt();

      if (args) {
        ret = functionFactory(functionId, args);
      }

      if ( ! ret) {
        std::string nameString;
        if (obj.hasNonNullKey("NAME")) {
          nameString = obj.valueForKey("NAME").asStr();
        }
        else {
          nameString = "?";
        }
        raiseFunctionFactoryError(nameString, functionId, args);
      }
    }
    break;

    case (EXPRESSION_TYPE_VALUE_VECTOR): {
      // Parse whatever.Is needed out of obj and pass the pieces to inListFactory
      // to make it easier to unit test independently of the parsing.
      // The first argumenthis used as the list element type.
      // If the ValueType of the .Ist builder expression needs to be "ARRAY" or something else,
      // a separate element type attribute will have to be serialized and passed in here.
      ret = vectorFactory(vt, args);
    }
    break;

    // Constant Values, parameters, tuples
    case (EXPRESSION_TYPE_VALUE_CONSTANT):
            ret = constantValueFactory(obj, vt, et, lc, rc);
    break;

    case (EXPRESSION_TYPE_VALUE_PARAMETER):
            ret = parameterValueFactory(obj, et, lc, rc);
    break;

    case (EXPRESSION_TYPE_VALUE_TUPLE):
            ret = tupleValueFactory(obj, et, lc, rc);
    break;

    case (EXPRESSION_TYPE_VALUE_TUPLE_ADDRESS):
            ret = new TupleAddressExpression();
    break;
    case (EXPRESSION_TYPE_VALUE_SCALAR):
            ret = new ScalarValueExpression(lc);
    break;
    case (EXPRESSION_TYPE_HASH_RANGE):
            ret = hashRangeFactory(obj);
    break;
    case (EXPRESSION_TYPE_OPERATOR_CASE_WHEN):
            ret = caseWhenFactory(vt, lc, rc);
    break;
    case (EXPRESSION_TYPE_OPERATOR_ALTERNATIVE):
            ret = new OperatorAlternativeExpression(lc, rc);
    break;

    // Subquery
    case (EXPRESSION_TYPE_ROW_SUBQUERY):
    case (EXPRESSION_TYPE_SELECT_SUBQUERY):
    ret = subqueryFactory(et, obj, args);
    break;

    // must handle all known expression in this factory
    default:

      char message[256];
      snprintf(message,256, "Invalid ExpressionType '%s' (%d) requested from factory",
               ExpressionTypeToString(et).c_str(), (int)et);
      throw Exception( message);
  }

  ret->setValueType(vt);
  ret->setValueSize(vs);
  // written thusly to ease testing/inspecting return content.
  LOG_TRACE("Created expression %p", ret);
  return ret;
}

boost::shared_array<int>
ExpressionUtil::convertIfAllTupleValues(const std::vector<AbstractExpression*> &expression)
{
  size_t cnt = expression.size();
  boost::shared_array<int> ret(new int[cnt]);
  for (int i = 0; i < cnt; ++i) {
    TupleValueExpression* casted=
        dynamic_cast<TupleValueExpression*>(expression[i]);
    if (casted == NULL) {
      return boost::shared_array<int>();
    }
    ret[i] = casted->GetColumnId();
  }
  return ret;
}

boost::shared_array<int>
ExpressionUtil::convertIfAllParameterValues(const std::vector<AbstractExpression*> &expression)
{
  size_t cnt = expression.size();
  boost::shared_array<int> ret(new int[cnt]);
  for (int i = 0; i < cnt; ++i) {
    ParameterValueExpression *casted =
        dynamic_cast<ParameterValueExpression*>(expression[i]);
    if (casted == NULL) {
      return boost::shared_array<int>();
    }
    ret[i] = casted->GetParameterId();
  }
  return ret;
}

void
ExpressionUtil::extractTupleValuesColumnIdx(const AbstractExpression* expr, std::vector<int> &columnIds)
{
  if (expr == NULL)
  {
    return;
  }
  if(expr->GetExpressionType() == EXPRESSION_TYPE_VALUE_TUPLE)
  {
    const TupleValueExpression* tve = dynamic_cast<const TupleValueExpression*>(expr);
    assert(tve != NULL);
    columnIds.push_back(tve->GetColumnId());
    return;
  }
  // Recurse
  ExpressionUtil::extractTupleValuesColumnIdx(expr->GetLeft(), columnIds);
  ExpressionUtil::extractTupleValuesColumnIdx(expr->GetRight(), columnIds);
}

void ExpressionUtil::loadIndexedExprsFromJson(std::vector<AbstractExpression*>& indexed_exprs, const std::string& jsonarraystring)
{
  PlannerDomRoot domRoot(jsonarraystring.c_str());
  PlannerDomValue expressionArray = domRoot.rootObject();
  for (int i = 0; i < expressionArray.arrayLen(); i++) {
    PlannerDomValue exprValue = expressionArray.valueAtIndex(i);
    AbstractExpression *expr = AbstractExpression::buildExpressionTree(exprValue);
    indexed_exprs.push_back(expr);
  }
}

AbstractExpression* ExpressionUtil::loadExpressionFromJson(const std::string& jsonstring)
{
  PlannerDomRoot domRoot(jsonstring.c_str());
  return AbstractExpression::buildExpressionTree(domRoot.rootObject());
}

}  // End expression namespace
}  // End peloton namespace

