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

std::unordered_map<std::string, BuiltInFuncType> BuiltInFunctions::kFuncMap;

void BuiltInFunctions::AddFunction(const std::string &func_name,
                                   BuiltInFuncType func) {
  kFuncMap.emplace(func_name, func);
}

BuiltInFuncType BuiltInFunctions::GetFuncByName(const std::string &func_name) {
  auto func = kFuncMap.find(func_name);
  if (func == kFuncMap.end()) {
    return {OperatorId::Invalid, nullptr};
  }
  return func->second;
}

}  // namespace function
}  // namespace peloton
