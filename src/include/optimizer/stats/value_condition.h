//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// value_condition.h
//
// Identification: src/include/optimizer/stats/value_condition.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/logger.h"
#include "type/value.h"
#include "common/internal_types.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// ValueCondition
// SELECT * FROM table WHERE [id = 1] <- ValueCondition
//===--------------------------------------------------------------------===//
class ValueCondition {
 public:
  oid_t column_id;
  std::string column_name;
  ExpressionType type;
  type::Value value;

  // Only with id. Default column_name to empty string.
  ValueCondition(oid_t column_id, ExpressionType type, const type::Value& value)
      : ValueCondition(column_id, "", type, value) {}

  // Only with column name. Default column_id to be 1.
  ValueCondition(std::string column_name, ExpressionType type,
                 const type::Value& value)
      : ValueCondition(0, column_name, type, value) {}

  ValueCondition(oid_t column_id, std::string column_name, ExpressionType type,
                 const type::Value& value)
      : column_id{column_id},
        column_name{column_name},
        type{type},
        value{value} {}
};

}  // namespace optimizer
}  // namespace peloton
