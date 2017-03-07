//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updateable_storage.cpp
//
// Identification: src/codegen/updateable_storage.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updateable_storage.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Add the given type to the storage format. We return the index that this value
// can be found it (i.e., which index to pass into Get() to get the value)
//===----------------------------------------------------------------------===//
uint32_t UpdateableStorage::Add(type::Type::TypeId type) {
  types_.push_back(type);
  return types_.size() - 1;
}

llvm::Type* UpdateableStorage::Finalize(CodeGen& codegen) {
  // Return the constructed type if we've already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  std::vector<llvm::Type*> llvm_types;
  for (uint32_t i = 0; i < types_.size(); i++) {
    llvm::Type* val_type = nullptr;
    llvm::Type* len_type = nullptr;
    codegen::Value::TypeForMaterialization(codegen, types_[i], val_type,
                                           len_type);
    // Create an entry for the value and add the type to the struct type we're
    // constructing
    uint32_t val_type_size = codegen.SizeOf(val_type);
    storage_format_.push_back(
        CompactStorage::EntryInfo{val_type, i, false, val_type_size});
    llvm_types.push_back(val_type);
    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the value and add the type to the struct type we're
      // constructing
      uint32_t len_type_size = codegen.SizeOf(len_type);
      storage_format_.push_back(
          CompactStorage::EntryInfo{len_type, i, true, len_type_size});
      llvm_types.push_back(len_type);
    }
  }
  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = codegen.SizeOf(storage_type_);
  return storage_type_;
}

// Get the value at a specific index into the storage area
codegen::Value UpdateableStorage::Get(CodeGen& codegen, llvm::Value* area_start,
                                      uint64_t index) const {
  assert(storage_type_ != nullptr);
  assert(index < types_.size());
  int val_idx = -1, len_idx = -1;
  for (uint64_t i = 0; i < storage_format_.size(); i++) {
    if (storage_format_[i].index == index) {
      if (storage_format_[i].is_length) {
        len_idx = i;
      } else {
        val_idx = i;
      }
    }
  }
  assert(val_idx >= 0);
  llvm::Value* typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());
  llvm::Value* val = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
      storage_type_, typed_ptr, 0, val_idx));
  llvm::Value* len = nullptr;
  if (len_idx > 0) {
    len = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, len_idx));
  }
  return codegen::Value{types_[index], val, len};
}

// Get the value at a specific index into the storage area
void UpdateableStorage::Set(CodeGen& codegen, llvm::Value* area_start,
                            uint64_t index, const codegen::Value& value) const {
  llvm::Value* val = nullptr;
  llvm::Value* len = nullptr;
  value.ValuesForMaterialization(codegen, val, len);

  int val_idx = -1, len_idx = -1;
  for (uint64_t i = 0; i < storage_format_.size(); i++) {
    if (storage_format_[i].index == index) {
      if (storage_format_[i].is_length) {
        len_idx = i;
      } else {
        val_idx = i;
      }
    }
  }
  llvm::Value* typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());
  // Store the value and len at the appropriate slots
  codegen->CreateStore(val, codegen->CreateConstInBoundsGEP2_32(
                                storage_type_, typed_ptr, 0, val_idx));
  if (len != nullptr) {
    codegen->CreateStore(len, codegen->CreateConstInBoundsGEP2_32(
                                  storage_type_, typed_ptr, 0, len_idx));
  }
}

}  // namespace codegen
}  // namespace peloton