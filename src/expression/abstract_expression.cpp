/*-------------------------------------------------------------------------
 *
 * abstract_expression.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/expression/abstract_expression.cpp
 *
 *-------------------------------------------------------------------------
 */


#include "expression/abstract_expression.h"

#include "common/types.h"
#include "common/serializer.h"
#include "expression/expression_util.h"

#include <sstream>
#include <cassert>
#include <stdexcept>

namespace nstore {
namespace expression {


// ------------------------------------------------------------------
// AbstractExpression
// ------------------------------------------------------------------
AbstractExpression::AbstractExpression()
: m_left(NULL), m_right(NULL),
  m_type(EXPRESSION_TYPE_INVALID),
  m_hasParameter(true)
{
}

AbstractExpression::AbstractExpression(ExpressionType type)
: m_left(NULL), m_right(NULL),
  m_type(type),
  m_hasParameter(true)
{
}

AbstractExpression::AbstractExpression(ExpressionType type,
                                       AbstractExpression* left,
                                       AbstractExpression* right)
: m_left(left), m_right(right),
  m_type(type),
  m_hasParameter(true)
{
}

AbstractExpression::~AbstractExpression()
{
  delete m_left;
  delete m_right;
}

void
AbstractExpression::substitute(const ValueArray &params)
{
  if (!m_hasParameter)
    return;

  // descend. nodes with parameters overload substitute()
  //VOLT_TRACE("Substituting parameters for expression \n%s ...", debug(true).c_str());
  if (m_left) {
    //VOLT_TRACE("Substitute processing left child...");
    m_left->substitute(params);
  }
  if (m_right) {
    //VOLT_TRACE("Substitute processing right child...");
    m_right->substitute(params);
  }
}

bool
AbstractExpression::hasParameter() const
{
  if (m_left && m_left->hasParameter())
    return true;
  return (m_right && m_right->hasParameter());
}

bool
AbstractExpression::initParamShortCircuits()
{
  if (m_left && m_left->hasParameter())
    return true;
  if (m_right && m_right->hasParameter())
    return true;

  m_hasParameter = false;
  return false;
}

std::string
AbstractExpression::debug() const
{
  std::ostringstream buffer;
  buffer << "Expression[" << ExpressionToString(getExpressionType()) << ", " << getExpressionType() << "]";
  return (buffer.str());
}

std::string
AbstractExpression::debug(bool traverse) const
{
  return (traverse ? debug(std::string("")) : debug());
}

std::string
AbstractExpression::debug(const std::string &spacer) const
{
  std::ostringstream buffer;
  buffer << spacer << "+ " << debug() << "\n";

  std::string info_spacer = spacer + "   ";
  buffer << debugInfo(info_spacer);

  // process children
  if (m_left != NULL || m_right != NULL) {
    buffer << info_spacer << "left:  " <<
        (m_left != NULL  ? "\n" + m_left->debug(info_spacer)  : "<NULL>\n");
    buffer << info_spacer << "right: " <<
        (m_right != NULL ? "\n" + m_right->debug(info_spacer) : "<NULL>\n");
  }
  return (buffer.str());
}

// ------------------------------------------------------------------
// SERIALIZATION METHODS
// ------------------------------------------------------------------
AbstractExpression*
AbstractExpression::buildExpressionTree(json_spirit::Object &obj)
{
  AbstractExpression * exp =
      AbstractExpression::buildExpressionTree_recurse(obj);

  if (exp)
    exp->initParamShortCircuits();
  return exp;
}

AbstractExpression*
AbstractExpression::buildExpressionTree_recurse(json_spirit::Object &obj)
{
  // build a tree recursively from the bottom upwards.
  // when the expression node is instantiated, its type,
  // value and child types will have been discovered.

  ExpressionType peek_type = EXPRESSION_TYPE_INVALID;
  ValueType value_type = VALUE_TYPE_INVALID;
  AbstractExpression *left_child = NULL;
  AbstractExpression *right_child = NULL;

  // read the expression type
  json_spirit::Value expressionTypeValue = json_spirit::find_value(obj,
                                                                   "TYPE");
  if (expressionTypeValue == json_spirit::Value::null) {
    throw ExpressionException("AbstractExpression:: buildExpressionTree_recurse:"
        "Couldn't find TYPE value");
  }

  assert(StringToExpression(expressionTypeValue.get_str()) != EXPRESSION_TYPE_INVALID);
  peek_type = StringToExpression(expressionTypeValue.get_str());

  // and the value type
  json_spirit::Value valueTypeValue = json_spirit::find_value(obj,
                                                              "VALUE_TYPE");
  if (valueTypeValue == json_spirit::Value::null) {
    throw ExpressionException("AbstractExpression:: buildExpressionTree_recurse:"
                                  " Couldn't find VALUE_TYPE value");
  }
  std::string valueTypeString = valueTypeValue.get_str();
  value_type = StringToValue(valueTypeString);

  // this should be relatively safe, though it ignores overflow.
  if ((value_type == VALUE_TYPE_TINYINT)  ||
      (value_type == VALUE_TYPE_SMALLINT) ||
      (value_type == VALUE_TYPE_INTEGER))
  {
    value_type = VALUE_TYPE_BIGINT;
  }

  assert(value_type != VALUE_TYPE_INVALID);

  // add the value size
  json_spirit::Value valueSizeValue = json_spirit::find_value(obj,
                                                              "VALUE_SIZE");
  if (valueSizeValue == json_spirit::Value::null) {
    throw ExpressionException("AbstractExpression:: buildExpressionTree_recurse:"
        " Couldn't find VALUE_SIZE value");
  }

  int valueSize = valueSizeValue.get_int();

  // recurse to children
  try {
    json_spirit::Value leftValue = json_spirit::find_value(obj, "LEFT");
    if (!(leftValue == json_spirit::Value::null)) {
      left_child = AbstractExpression::buildExpressionTree_recurse(leftValue.get_obj());
    } else {
      left_child = NULL;
    }

    json_spirit::Value rightValue = json_spirit::find_value( obj, "RIGHT");
    if (!(rightValue == json_spirit::Value::null)) {
      right_child = AbstractExpression::buildExpressionTree_recurse(rightValue.get_obj());
    } else {
      right_child = NULL;
    }

    // invoke the factory. obviously it has to handle null children.
    // pass it the serialization stream in case a subclass has more
    // to read. yes, the per-class data really does follow the
    // child serializations.
    return expressionFactory(obj,
                             peek_type, value_type, valueSize,
                             left_child, right_child);
  }
  catch (ExpressionException &ex) {
    delete left_child;
    delete right_child;
    throw;
  }
}

} // End expression namespace
} // End nstore namespace
