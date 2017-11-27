//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functions.h
//
// Identification: src/include/function/functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include <vector>

#include "type/value.h"

namespace peloton {
namespace function {

struct BuiltInFuncType {
  typedef type::Value (*Func)(const std::vector<type::Value> &);

  OperatorId op_id;
  Func impl;

  BuiltInFuncType(OperatorId _op_id, Func _impl) : op_id(_op_id), impl(_impl) {}
  BuiltInFuncType() : BuiltInFuncType(OperatorId::Invalid, nullptr) {}
};

class BuiltInFunctions {
 private:
  // Map the function name in C++ source (should be unique) to the actual
  // function implementation
  static std::unordered_map<std::string, BuiltInFuncType> kSourceNameFuncMap;

  // Map the function name used in SQL to the actual function implementation.
  // Different SQL names can map to the same function implementation(alias).
  static std::unordered_map<std::string, BuiltInFuncType> kSQLNameFuncMap;

 public:
  static void AddFunction(const std::string &sql_func_name,
                          const std::string &source_func_name,
                          BuiltInFuncType func);

  // Get the function from the name in C++ source code
  static BuiltInFuncType GetFuncBySourceName(const std::string &func_name);

  // Get the function from the name used in SQL
  static BuiltInFuncType GetFuncBySQLName(const std::string &func_name);
};

}  // namespace function
}  // namespace peloton
