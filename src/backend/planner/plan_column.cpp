//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// plan_column.cpp
//
// Identification: src/backend/planner/plan_column.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/planner/plan_column.h"

#include <sstream>

namespace peloton {
namespace planner {

PlanColumn::PlanColumn(json_spirit::Object &col_object)
    : m_col_object(col_object) {
  bool contains_name = false;
  bool contains_type = false;
  bool contains_size = false;
  bool contains_input_column_name = false;

  for (uint attr = 0; attr < m_col_object.size(); attr++) {
    if (m_col_object[attr].name_ == "GUID") {
      m_guid = m_col_object[attr].value_.get_int();
    } else if (m_col_object[attr].name_ == "NAME") {
      contains_name = true;
      m_name = m_col_object[attr].value_.get_str();
    } else if (m_col_object[attr].name_ == "TYPE") {
      contains_type = true;
      std::string m_col_object_type_string =
          m_col_object[attr].value_.get_str();
      m_type = StringToValueType(m_col_object_type_string);
    } else if (m_col_object[attr].name_ == "SIZE") {
      contains_size = true;
      m_size = m_col_object[attr].value_.get_int();
    } else if (m_col_object[attr].name_ == "INPUT_COLUMN_NAME") {
      contains_input_column_name = true;
      m_inputColumnName = m_col_object[attr].value_.get_str();
    }
  }

  if (!contains_input_column_name) {
    m_inputColumnName = "";
  }

  assert(contains_name && contains_type && contains_size);
}

int PlanColumn::GetGuid() const { return m_guid; }

std::string PlanColumn::GetColumnName() const { return m_name; }

ValueType PlanColumn::GetType() const { return m_type; }

size_t PlanColumn::GetSize() const { return m_size; }

std::string PlanColumn::GetInputColumnName() const { return m_inputColumnName; }

expression::AbstractExpression *PlanColumn::GetExpression() {
  expression::AbstractExpression *expression = nullptr;
  // lazy vector search

  json_spirit::Value column_expression_value =
      find_value(m_col_object, "EXPRESSION");
  if (column_expression_value == json_spirit::Value::null) {
    throw PlannerException(
        "PlanColumn::getExpression: Can't find EXPRESSION value");
  }

  json_spirit::Object column_expression_object =
      column_expression_value.get_obj();
  expression = expression::AbstractExpression::CreateExpressionTree(
      column_expression_object);

  // Hacky...shouldn't be calling this if we don't have an expression, though.
  assert(expression != nullptr);
  return expression;
}

std::ostream &operator<<(std::ostream &os, const PlanColumn &column) {
  os << "PlanColumn(";
  os << "guid=" << column.m_guid << ", ";
  os << "name=" << column.m_name << ", ";
  os << "type=" << ValueTypeToString(column.m_type) << ", ";
  os << "size=" << column.m_size << ")";

  return os;
}

}  // namespace planner
}  // namespace peloton
