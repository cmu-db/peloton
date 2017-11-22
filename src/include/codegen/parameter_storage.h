//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// parameter_storage.h
//
// Identification: src/include/codegen/parameter_storage.h
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
// A parameter storage which stores the parameterized values for codegen runtime
//===----------------------------------------------------------------------===//
class ParameterStorage {
 public:
  // Constructor
  ParameterStorage(std::vector<expression::Parameter> &parameters) :
      space_ptr_(nullptr), parameters_(parameters) {}

  // Build up the parameter storage and load up the values
  llvm::Type *Setup(CodeGen &codegen);

  // Set the parameter values
  void SetValues(CodeGen &codegen, llvm::Value *query_parameters_ptr,
                 llvm::Value *space_ptr);

  // Get the codegen value for the specific index
  codegen::Value GetValue(CodeGen &codegen, uint32_t index) const;

 private:
  codegen::Value DeriveParameterValue(CodeGen &codegen,
      expression::Parameter &parameter, llvm::Value *query_parameters_ptr,
      uint32_t index);

 private:
  // Storage format
  UpdateableStorage storage_;

  // Storage space
  llvm::Value *space_ptr_;

  // Parameters' information
  std::vector<expression::Parameter> &parameters_;
};

}  // namespace codegen
}  // namespace peloton
