//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// local_variable.h
//
// Identification: src/include/codegen/lang/local_variable.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {
namespace lang {

class LocalVariable {
 public:
  LocalVariable(CodeGen &codegen, llvm::Type *type);

  llvm::Value *GetValue() const { return value_; }

 private:
  llvm::Value *value_;
};

}  // namespace lang
}  // namespace codegen
}  // namespace peloton
