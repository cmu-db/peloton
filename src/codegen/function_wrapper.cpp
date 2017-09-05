//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_wrapper.cpp
//
// Identification: src/codegen/function_wrapper.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include <stdarg.h>
#include "codegen/function_wrapper.h"
#include "type/value_factory.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

inline std::vector<peloton::type::Value> GetArguments(
    int n_args, va_list &ap) {
  std::vector<peloton::type::Value> args;
  for (int i = 0; i < n_args; ++i) {
    peloton::type::TypeId type_id = peloton::type::TypeId(va_arg(ap, int32_t));
    switch (type_id) {
      case peloton::type::TypeId::TINYINT:
        args.push_back(peloton::type::ValueFactory::GetTinyIntValue(
            va_arg(ap, int32_t)
        ));
        break;
      case peloton::type::TypeId::SMALLINT:
        args.push_back(peloton::type::ValueFactory::GetSmallIntValue(
            va_arg(ap, int32_t)
        ));
        break;
      case peloton::type::TypeId::INTEGER:
        args.push_back(peloton::type::ValueFactory::GetIntegerValue(
            va_arg(ap, int32_t)
        ));
        break;
      case peloton::type::TypeId::BIGINT:
        args.push_back(peloton::type::ValueFactory::GetBigIntValue(
            va_arg(ap, int64_t)
        ));
        break;
      case peloton::type::TypeId::DECIMAL:
        args.push_back(peloton::type::ValueFactory::GetDecimalValue(
            va_arg(ap, double)
        ));
        break;
      case peloton::type::TypeId::DATE:
        args.push_back(peloton::type::ValueFactory::GetDateValue(
            va_arg(ap, int32_t)
        ));
        break;
      case peloton::type::TypeId::TIMESTAMP:
        args.push_back(peloton::type::ValueFactory::GetTimestampValue(
            va_arg(ap, int64_t)
        ));
        break;
      case peloton::type::TypeId::VARCHAR:
        args.push_back(peloton::type::ValueFactory::GetVarcharValue(
            va_arg(ap, const char*)
        ));
        break;
      default:
        args.push_back(peloton::type::ValueFactory::GetNullValueByType(type_id));
        break;
    }
  }
  return args;
}

int8_t FunctionWrapper::TinyIntWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int8_t>();
}

int16_t FunctionWrapper::SmallIntWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int16_t>();
}

int32_t FunctionWrapper::IntegerWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int32_t>();
}

int64_t FunctionWrapper::BigIntWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int64_t>();
}

double FunctionWrapper::DecimalWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<double>();
}

int32_t FunctionWrapper::DateWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int32_t>();
}

int64_t FunctionWrapper::TimestampWrapper(int64_t func, int n_args, ...) {
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  return ret.GetAs<int64_t>();
}

const char* FunctionWrapper::VarcharWrapper(int64_t func, int n_args, ...) {
  // TODO: put this pool to a proper place
  static type::EphemeralPool pool;
  va_list ap;
  va_start(ap, n_args);
  std::vector<peloton::type::Value> args = GetArguments(n_args, ap);
  peloton::type::Value ret = ((BuiltInFuncType)func)(args);
  char* str = static_cast<char*>(pool.Allocate(ret.GetLength()));
  strcpy(str, ret.GetData());
  return str;
}

}  // namespace codegen
}  // namespace peloton
