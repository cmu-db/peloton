//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// vector.h
//
// Identification: src/include/codegen/vector.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>

#include "codegen/codegen.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A vector of fixed size holding elements of a homogeneous type
//===----------------------------------------------------------------------===//
class Vector {
 public:
  static std::atomic<uint32_t> kDefaultVectorSize;
  static uint32_t kDefaultVectorAlignment;

  // Constructors
  Vector(llvm::Value *vector, uint32_t vector_size, llvm::Type *element_type);

  // Set the value at the provided index
  void SetValue(CodeGen &codegen, llvm::Value *index, llvm::Value *item) const;

  llvm::Value *GetPtrToValue(CodeGen &codegen, llvm::Value *index) const;

  // Get the value at the provided index
  llvm::Value *GetValue(CodeGen &codegen, llvm::Value *index) const;

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  llvm::Value *GetVectorPtr() const { return vector_ptr_; }

  uint32_t GetCapacity() const { return capacity_; }

  llvm::Value *GetNumElements() const { return num_elements_; }

  void SetNumElements(llvm::Value *num_elements) {
    num_elements_ = num_elements;
  }

 private:
  // The buffer pointer
  llvm::Value *vector_ptr_;

  // The maximum capacity of this vector
  uint32_t capacity_;

  // The types of elements store in this vector
  llvm::Type *element_type_;

  // The number of elements actually stored in this vector
  llvm::Value *num_elements_;
};

}  // namespace codegen
}  // namespace peloton