//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// auxiliary_producer_function.h
//
// Identification: src/include/codegen/auxiliary_producer_function.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/function_builder.h"

namespace peloton {
namespace codegen {

class AuxiliaryProducerFunction {
 public:
  // Constructor
  explicit AuxiliaryProducerFunction();
  explicit AuxiliaryProducerFunction(const FunctionDeclaration &declaration);

  // Call the function
  llvm::Value *Call(CodeGen &codegen) const;

 private:
  // The function
  llvm::Function *function_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation
///
////////////////////////////////////////////////////////////////////////////////

inline AuxiliaryProducerFunction::AuxiliaryProducerFunction()
    : function_(nullptr) {}

inline AuxiliaryProducerFunction::AuxiliaryProducerFunction(
    const FunctionDeclaration &declaration)
    : function_(declaration.GetDeclaredFunction()) {}

inline llvm::Value *AuxiliaryProducerFunction::Call(CodeGen &codegen) const {
  // At this point, the function cannot be NULL!
  PL_ASSERT(function_ != nullptr);
  auto *runtime_state_ptr = codegen.GetState();
  return codegen.CallFunc(function_, {runtime_state_ptr});
}

}  // namespace codegen
}  // namespace peloton