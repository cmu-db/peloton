//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_expression.cpp
//
// Identification: src/expression/abstract_expression.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/abstract_tuple.h"
#include "common/types.h"
#include "common/value.h"
#include "common/logger.h"
#include "common/serializer.h"
#include "common/macros.h"
#include "expression/abstract_expression.h"
#include "executor/executor_context.h"
#include "expression/expression_util.h"

namespace peloton {
namespace expression {

AbstractExpression::AbstractExpression(ExpressionType expr_type)
    : m_type(expr_type),
      m_valueType(VALUE_TYPE_INVALID),
      m_hasParameter(true) {}

AbstractExpression::AbstractExpression(ExpressionType expr_type, ValueType type,
                                       AbstractExpression *left,
                                       AbstractExpression *right)
    : m_type(expr_type),
      m_valueType(type),
      m_left(left),
      m_right(right),
      m_hasParameter(true) {}

AbstractExpression::AbstractExpression(ExpressionType expr_type, ValueType type)
    : AbstractExpression(expr_type, type, nullptr, nullptr) {}

AbstractExpression::~AbstractExpression() {
  if (m_left != nullptr) {
    delete m_left;
  }
  if (m_right != nullptr) {
    delete m_right;
  }
  // Parser variables need to be cleaned too
  delete name;
  delete column;
  delete alias;
}

bool AbstractExpression::HasParameter() const {
  if (m_left && m_left->HasParameter()) return true;
  return (m_right && m_right->HasParameter());
}

bool AbstractExpression::InitParamShortCircuits() {
  return (m_hasParameter = HasParameter());
}

std::string AbstractExpression::Debug() const {
  if (this == nullptr) return "";
  std::ostringstream buffer;
  buffer << "Expression[" << ExpressionTypeToString(GetExpressionType()) << ", "
         << GetExpressionType() << "]";
  return (buffer.str());
}

std::string AbstractExpression::Debug(bool traverse) const {
  return (traverse ? Debug(std::string("")) : Debug());
}

std::string AbstractExpression::Debug(const std::string &spacer) const {
  std::ostringstream buffer;
  buffer << spacer << "+ " << Debug() << "\n";

  std::string info_spacer = spacer + "   ";
  buffer << DebugInfo(info_spacer);

  // process children
  if (m_left != NULL || m_right != NULL) {
    buffer << info_spacer << "left:  "
           << (m_left != NULL ? "\n" + m_left->Debug(info_spacer) : "<NULL>\n");
    buffer << info_spacer
           << "right: " << (m_right != NULL ? "\n" + m_right->Debug(info_spacer)
                                            : "<NULL>\n");
  }
  return buffer.str();
}

bool AbstractExpression::SerializeTo(SerializeOutput &output
                                         UNUSED_ATTRIBUTE) const {
  PL_ASSERT(&output != nullptr);
  return false;
}

bool AbstractExpression::DeserializeFrom(SerializeInputBE &input
                                             UNUSED_ATTRIBUTE) const {
  PL_ASSERT(&input != nullptr);
  return false;
}

//===--------------------------------------------------------------------===//
// Actual Constructors
//===--------------------------------------------------------------------===//

const std::string AbstractExpression::GetInfo() const {
  std::ostringstream os;

  os << Debug();

  return os.str();
}

}  // End expression namespace
}  // End peloton namespace
