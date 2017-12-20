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

// Built-in func map
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

// PL/pgSQL UDF map
std::unordered_map<oid_t, std::shared_ptr<peloton::codegen::CodeContext>>
    PlpgsqlFunctions::kFuncMap;

void PlpgsqlFunctions::AddFunction(
    const oid_t oid,
    std::shared_ptr<peloton::codegen::CodeContext> func_context) {
  kFuncMap.emplace(oid, func_context);
}

std::shared_ptr<peloton::codegen::CodeContext>
PlpgsqlFunctions::GetFuncContextByOid(const oid_t oid) {
  auto func = kFuncMap.find(oid);
  if (func == kFuncMap.end()) {
    return nullptr;
  }
  return func->second;
}

}  // namespace function
}  // namespace peloton
