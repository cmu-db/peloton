//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// abstract_expression.cpp
//
// Identification: src/backend/expression/abstract_expression.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>
#include <cassert>
#include <stdexcept>

#include "backend/common/logger.h"
#include "backend/common/serializer.h"
#include "backend/common/types.h"
#include "backend/expression/expression_util.h"
#include "backend/expression/abstract_expression.h"

namespace peloton {
namespace expression {

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

bool
AbstractExpression::HasParameter() const
{
  if (m_left && m_left->HasParameter())
    return true;
  return (m_right && m_right->HasParameter());
}

bool
AbstractExpression::InitParamShortCircuits()
{
  return (m_hasParameter = HasParameter());
}

std::string
AbstractExpression::Debug() const
{
  std::ostringstream buffer;
  buffer << "Expression[" << ExpressionTypeToString(GetExpressionType()) << ", " << GetExpressionType() << "]";
  return (buffer.str());
}

std::string
AbstractExpression::Debug(bool traverse) const
{
  return (traverse ? Debug(std::string("")) : Debug());
}

std::string
AbstractExpression::Debug(const std::string &spacer) const
{
  std::ostringstream buffer;
  buffer << spacer << "+ " << Debug() << "\n";

  std::string info_spacer = spacer + "   ";
  buffer << DebugInfo(info_spacer);

  // process children
  if (m_left != NULL || m_right != NULL) {
    buffer << info_spacer << "left:  " <<
        (m_left != NULL  ? "\n" + m_left->Debug(info_spacer)  : "<NULL>\n");
    buffer << info_spacer << "right: " <<
        (m_right != NULL ? "\n" + m_right->Debug(info_spacer) : "<NULL>\n");
  }
  return (buffer.str());
}

//===--------------------------------------------------------------------===//
// Actual Constructors
//===--------------------------------------------------------------------===//

AbstractExpression *AbstractExpression::CreateExpressionTree(
    json_spirit::Object &obj) {
  AbstractExpression *expr =
      AbstractExpression::CreateExpressionTreeRecurse(obj);

  if (expr) expr->InitParamShortCircuits();

  return expr;
}

AbstractExpression *AbstractExpression::CreateExpressionTreeRecurse(
    json_spirit::Object &obj) {
  // build a tree recursively from the bottom upwards.
  // when the expression node is instantiated, its type,
  // value and child types will have been discovered.

  ExpressionType peek_type = EXPRESSION_TYPE_INVALID;
  ValueType value_type = VALUE_TYPE_INVALID;
  AbstractExpression *left_child = nullptr;
  AbstractExpression *right_child = nullptr;

  // read the expression type
  json_spirit::Value expression_type_value =
      json_spirit::find_value(obj, "TYPE");

  if (expression_type_value == json_spirit::Value::null) {
    throw ExpressionException(
        "AbstractExpression:: buildExpressionTree_recurse:"
        "Couldn't find TYPE value");
  }

  assert(StringToExpressionType(expression_type_value.get_str()) !=
         EXPRESSION_TYPE_INVALID);
  peek_type = StringToExpressionType(expression_type_value.get_str());

  // and the value type
  json_spirit::Value valueTypeValue =
      json_spirit::find_value(obj, "VALUE_TYPE");
  if (valueTypeValue == json_spirit::Value::null) {
    throw ExpressionException(
        "AbstractExpression:: buildExpressionTree_recurse:"
        " Couldn't find VALUE_TYPE value");
  }

  std::string value_type_string = valueTypeValue.get_str();
  value_type = StringToValueType(value_type_string);

  // this should be relatively safe, though it ignores overflow.
  if ((value_type == VALUE_TYPE_TINYINT) ||
      (value_type == VALUE_TYPE_SMALLINT) ||
      (value_type == VALUE_TYPE_INTEGER)) {
    value_type = VALUE_TYPE_BIGINT;
  }

  assert(value_type != VALUE_TYPE_INVALID);

  // add the value size
  json_spirit::Value value_size_value =
      json_spirit::find_value(obj, "VALUE_SIZE");

  if (value_size_value == json_spirit::Value::null) {
    throw ExpressionException(
        "AbstractExpression:: buildExpressionTree_recurse:"
        " Couldn't find VALUE_SIZE value");
  }

  int value_size = value_size_value.get_int();

  // recurse to children
  try {
    json_spirit::Value leftValue = json_spirit::find_value(obj, "LEFT");

    if (!(leftValue == json_spirit::Value::null)) {
      left_child =
          AbstractExpression::CreateExpressionTreeRecurse(leftValue.get_obj());
    } else {
      left_child = nullptr;
    }

    json_spirit::Value rightValue = json_spirit::find_value(obj, "RIGHT");

    if (!(rightValue == json_spirit::Value::null)) {
      right_child =
          AbstractExpression::CreateExpressionTreeRecurse(rightValue.get_obj());
    } else {
      right_child = nullptr;
    }

    // Invoke the factory. obviously it has to handle null children.
    // pass it the serialization stream in case a subclass has more
    // to read. yes, the per-class data really does follow the
    // child serializations.

    return ExpressionFactory(obj, peek_type, value_type, value_size, left_child,
                             right_child);
  } catch (ExpressionException &ex) {
    // clean up children
    delete left_child;
    delete right_child;
    throw;
  }
}

std::ostream &operator<<(std::ostream &os, const AbstractExpression &expr) {
  os << expr.Debug();
  return os;
}

}  // End expression namespace
}  // End peloton namespace
