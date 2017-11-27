//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// functions.cpp
//
// Identification: src/function/functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/functions.h"

namespace peloton {
namespace function {

std::unordered_map<std::string, BuiltInFuncType>
    BuiltInFunctions::kSourceNameFuncMap;
std::unordered_map<std::string, BuiltInFuncType>
    BuiltInFunctions::kSQLNameFuncMap;

void BuiltInFunctions::AddFunction(const std::string &sql_func_name,
                                   const std::string &source_func_name,
                                   BuiltInFuncType func) {
  kSourceNameFuncMap.emplace(source_func_name, func);
  kSQLNameFuncMap.emplace(sql_func_name, func);
}

BuiltInFuncType BuiltInFunctions::GetFuncBySourceName(
    const std::string &func_name) {
  auto func = kSourceNameFuncMap.find(func_name);
  if (func == kSourceNameFuncMap.end()) {
    return {OperatorId::Invalid, nullptr};
  }
  return func->second;
}

BuiltInFuncType BuiltInFunctions::GetFuncBySQLName(
    const std::string &func_name) {
  auto func = kSQLNameFuncMap.find(func_name);
  if (func == kSQLNameFuncMap.end()) {
    return {OperatorId::Invalid, nullptr};
  }
  return func->second;
}

}  // namespace function
}  // namespace peloton
