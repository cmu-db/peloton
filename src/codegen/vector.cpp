//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vector.cpp
//
// Identification: src/codegen/vector.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/vector.h"

namespace peloton {
namespace codegen {

// The default vector size is 1024 elements
std::atomic<uint32_t> Vector::kDefaultVectorSize{1024};

// The default byte-alignment of all vectors is 32 bytes
uint32_t Vector::kDefaultVectorAlignment = 32;

// Constructor
Vector::Vector(llvm::Value *vector, uint32_t vector_size,
               llvm::Type *element_type)
    : vector_ptr_(vector),
      capacity_(vector_size),
      element_type_(element_type) {}

void Vector::SetValue(CodeGen &codegen, llvm::Value *index,
                      llvm::Value *item) const {
  codegen->CreateStore(item, GetPtrToValue(codegen, index));
}

llvm::Value *Vector::GetPtrToValue(CodeGen &codegen, llvm::Value *index) const {
  return codegen->CreateInBoundsGEP(element_type_, vector_ptr_, index);
}

llvm::Value *Vector::GetValue(CodeGen &codegen, llvm::Value *index) const {
  return codegen->CreateLoad(GetPtrToValue(codegen, index));
}

}  // namespace codegen
}  // namespace peloton