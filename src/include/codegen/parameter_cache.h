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
#include "codegen/updateable_storage.h"
#include "codegen/value.h"
#include "expression/parameter.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A parameter cache which stores the parameterized values for codegen runtime
//===----------------------------------------------------------------------===//
class ParameterCache {
 public:
  // Constructor
  ParameterCache(const std::vector<expression::Parameter> &parameters) :
      parameters_(parameters) {}

  // Set the parameter values
  void StoreValues(CodeGen &codegen, llvm::Value *query_parameters_ptr);

  // Get the codegen value for the specific index
  codegen::Value GetValue(uint32_t index) const;

 private:
  codegen::Value DeriveParameterValue(CodeGen &codegen,
      llvm::Value *query_parameters_ptr, uint32_t index,
      peloton::type::TypeId type_id, bool is_nullable);

 private:
  // Parameter information
  const std::vector<expression::Parameter> &parameters_;

  // Parameter value storage
  std::vector<codegen::Value> values_;
};

}  // namespace codegen
}  // namespace peloton
