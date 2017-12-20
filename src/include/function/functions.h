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

#include "codegen/code_context.h"
#include "codegen/value.h"
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
  static std::unordered_map<std::string, BuiltInFuncType> kFuncMap;

 public:
  static void AddFunction(const std::string &func_name, BuiltInFuncType func);

  // Get the function from the name in C++ source code
  static BuiltInFuncType GetFuncByName(const std::string &func_name);
};

typedef uint32_t oid_t;

class PlpgsqlFunctions {
 private:
  static std::unordered_map<oid_t,
                            std::shared_ptr<peloton::codegen::CodeContext>>
      kFuncMap;

 public:
  static void AddFunction(
      const oid_t oid,
      std::shared_ptr<peloton::codegen::CodeContext> code_context);

  static std::shared_ptr<peloton::codegen::CodeContext> GetFuncContextByOid(
      const oid_t oid);
};

}  // namespace function
}  // namespace peloton
