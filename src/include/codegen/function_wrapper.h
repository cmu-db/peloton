#pragma once

#include "type/types.h"
#include "type/value.h"

namespace peloton {
namespace codegen {

class FunctionWrapper {
  typedef peloton::type::Value (*BuiltInFuncType)(const std::vector<peloton::type::Value> &);
 public:
  static int8_t TinyIntWrapper(int64_t func, int n_args, ...);
  static int16_t SmallIntWrapper(int64_t func, int n_args, ...);
  static int32_t IntegerWrapper(int64_t func, int n_args, ...);
  static int64_t BigIntWrapper(int64_t func, int n_args, ...);
  static double DecimalWrapper(int64_t func, int n_args, ...);
  static int32_t DateWrapper(int64_t func, int n_args, ...);
  static int64_t TimestampWrapper(int64_t func, int n_args, ...);
  static const char *VarcharWrapper(int64_t func, int n_args, ...);
};

}
}
