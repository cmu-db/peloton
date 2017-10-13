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

typedef type::Value (*BuiltInFuncType)(const std::vector<type::Value> &);

class BuiltInFunctions {
 private:
  static std::unordered_map<std::string, BuiltInFuncType> kFuncMap;

 public:
  static void AddFunction(const std::string &func_name, BuiltInFuncType func);

  static BuiltInFuncType GetFuncByName(const std::string &func_name);
};

}  // namespace function
}  // namespace peloton
