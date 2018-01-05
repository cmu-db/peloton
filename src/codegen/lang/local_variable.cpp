//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// local_variable.cpp
//
// Identification: src/codegen/lang/local_variable.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/lang/local_variable.h"

namespace peloton {
namespace codegen {
namespace lang {

LocalVariable::LocalVariable(CodeGen &codegen, llvm::Type *type) {
  if (auto array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
    auto elem_type = array_type->getArrayElementType();
    auto nelems = codegen.Const32(array_type->getArrayNumElements());
    auto align = Vector::kDefaultVectorAlignment;

    llvm::AllocaInst *array = codegen->CreateAlloca(elem_type, nelems);

    array->setAlignment(align);

    value_ = array;
  } else {
    value_ = codegen->CreateAlloca(type);
  }
}

}  // namespace lang
}  // namespace codegen
}  // namespace peloton
