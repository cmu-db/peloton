//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_column.h
//
// Identification: src/backend/planner/plan_column.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/expression/abstract_expression.h"

#include <json_spirit.h>

namespace peloton {
namespace planner {

/**
 * Convenience class to deserialize a PlanColumn object from the JSON
 * and provide common accessors to the contents.  Currently relies on
 * colObject to remain valid;
 * PlanColumns should not be passed around, stored, or expected to be
 * valid outside the scope of the initial JSON deserialization.
 */
class PlanColumn : public Printable {
 public:
  PlanColumn(json_spirit::Object &col_object);

  int GetGuid() const;

  std::string GetColumnName() const;

  ValueType GetType() const;

  size_t GetSize() const;

  std::string GetInputColumnName() const;

  // Lazily evaluates the expression in the JSON object because some expressions
  // (namely aggregates) are currently unhappy, so we only actually do this from
  // places where we know it will succeed.
  expression::AbstractExpression *GetExpression();

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  const json_spirit::Object &m_col_object;

  oid_t m_guid;

  std::string m_name;

  ValueType m_type;

  size_t m_size;

  std::string m_inputColumnName;
};

}  // namespace planner
}  // namespace peloton
