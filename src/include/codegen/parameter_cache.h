//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_cache.h
//
// Identification: src/include/codegen/parameter_cache.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/query_parameters_map.h"
#include "codegen/value.h"
#include "expression/parameter.h"
#include "type/type.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter cache which stores the parameterized values for codegen runtime
//===----------------------------------------------------------------------===//
class ParameterCache {
 public:
  // Constructor
  explicit ParameterCache(const QueryParametersMap &map)
      : parameters_map_(map) {}

  // Set the parameter values
  void Populate(CodeGen &codegen, llvm::Value *query_parameters_ptr);

  // Get the cached value for the given expression
  codegen::Value GetValue(uint32_t index) const;
  codegen::Value GetValue(const expression::AbstractExpression *expr) const;

  // Clear all cache parameter values
  void Reset();

 private:
  static codegen::Value DeriveParameterValue(CodeGen &codegen,
                                             llvm::Value *query_parameters_ptr,
                                             uint32_t index,
                                             peloton::type::TypeId type_id,
                                             bool is_nullable);

 private:
  // Parameter information
  const QueryParametersMap &parameters_map_;

  // Parameter value storage
  std::vector<codegen::Value> values_;
};

}  // namespace codegen
}  // namespace peloton
