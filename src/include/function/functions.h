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

#include "function/date_functions.h"
#include "function/string_functions.h"
#include "function/decimal_functions.h"

#include <unordered_map>

namespace peloton {
namespace function {

typedef type::Value (*BuiltInFuncType)(const std::vector<type::Value> &);

class BuiltInFunctions {
  static std::unordered_map<std::string, BuiltInFuncType> func_map;

 public:

  static void AddFunction(const std::string &func_name, BuiltInFuncType func);

  static BuiltInFuncType GetFuncByName(const std::string &func_name);
};
}  // namespace function
}  // namespace peloton
