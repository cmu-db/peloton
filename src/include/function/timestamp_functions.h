//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timestamp_functions.h
//
// Identification: src/include/function/timestamp_functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "common/logger.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace function {

class TimestampFunctions {
 public:
  // Truncate a timestamp to specified precision
  // (1) The first argument selects to which precision to truncate the input
  // value
  // (2) The second argument is the length of the precision value (used to
  // evaluate whether the value is NULL)
  // (3) The third argument is the value expression of type timestamp
  // (4) The fourth argument is the length of the timestamp value (used to
  // evaluate whether the value is NULL)
  // @return The Value returned is the value of type timestamp with
  // all fields that are less significant than the selected one set to zero (or
  // one, for day and month).
  // TODO(lma): We do not support directly using date_trunc with a constant
  // value from the SQL string. We don't need to change the implementation in
  // this function here to support that, but we need the parser/binder to
  // support explicit type casts. For example:
  // SELECT date_trunc('hour', TIMESTAMP '2001-02-16 20:38:40');
  static uint64_t DateTrunc(uint32_t date_part_type, uint64_t value);
};

}  // namespace function
}  // namespace peloton
