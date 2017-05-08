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
#include "type/types.h"

namespace peloton{
namespace optimizer{

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

  // Only with id.
  ValueCondition(oid_t column_id, ExpressionType type, const type::Value& value)
  : column_id{column_id},
    column_name(""),
    type{type},
    value{value}
  {}

  ValueCondition(oid_t column_id, std::string column_name, ExpressionType type, type::Value value)
    : column_id{column_id},
      column_name{column_name},
      type{type},
      value{value}
  {}
};

} /* namespace optimizer */
} /* namespace peloton */
