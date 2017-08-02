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
  static void AddFunction(const std::string &func_name, BuiltInFuncType func) {
    func_map.emplace(func_name, func);
  }
  static FuncType GetFuncByName(const std::string &func_name) {
    auto func = func_map.find(func_name);
    if (func == func_map.end())
      return nullptr;
    return func->second;
  }
};
}
}
