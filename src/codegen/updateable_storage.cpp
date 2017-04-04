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

#include "codegen/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Add the given type to the storage format. We return the index that this value
// can be found it (i.e., which index to pass into Get() to get the value)
//===----------------------------------------------------------------------===//
uint32_t UpdateableStorage::AddType(type::Type::TypeId type) {
  PL_ASSERT(storage_type_ == nullptr);
  types_.push_back(type);
  return types_.size() - 1;
}

llvm::Type *UpdateableStorage::Finalize(CodeGen &codegen) {
  // Return the constructed type if we've already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  std::vector<llvm::Type *> llvm_types;
  const auto numitems = types_.size();

  // We do not keep all the EntryInfos for each bit since it is waste of memory
  llvm::Type *null_type = codegen.BoolType();
  for (uint32_t i = 0; i < numitems; i++)
    llvm_types.push_back(null_type);

  for (uint32_t i = 0; i < numitems; i++) {
    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, types_[i], val_type, len_type);

    // Create an entry for the value and add the type to the struct type we're
    // constructing
    uint32_t val_type_size = codegen.SizeOf(val_type);
    storage_format_.push_back(
        CompactStorage::EntryInfo{val_type, i, false, val_type_size});
    llvm_types.push_back(val_type);

    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the length and add to the struct
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
codegen::Value UpdateableStorage::GetValueAt(CodeGen &codegen,
                                             llvm::Value *ptr,
                                             uint64_t index) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(index < types_.size());

  // TODO: This linear search isn't great ...
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

  // Make sure we found something
  PL_ASSERT(val_idx >= 0);

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  const auto numitems = types_.size();
  llvm::Value *val = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
      storage_type_, typed_ptr, 0, numitems + val_idx));

  // If there is a length-component for this entry, load it too
  llvm::Value *len = nullptr;
  if (len_idx > 0) {
    len = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, numitems + len_idx));
  }

  llvm::Value *null = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
                                          storage_type_, typed_ptr, 0,
                                          index));

  // Done
  return codegen::Value{types_[index], val, len, null};
}

// Get the value at a specific index into the storage area
void UpdateableStorage::SetValueAt(CodeGen &codegen, llvm::Value *ptr,
                                   uint64_t index,
                                   const codegen::Value &value) const {
  llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
  value.ValuesForMaterialization(val, len, null);

  // Some data are coming in without its null bit allocated
  if (null == nullptr)
    null = codegen::Value::SetNullValue(codegen, value);

  // TODO: This linear search isn't great ...
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

  PL_ASSERT(value.GetValue()->getType() == storage_format_[val_idx].type);

  const auto numitems = types_.size();
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  // Store the value and len at the appropriate slots
  codegen->CreateStore(val, codegen->CreateConstInBoundsGEP2_32(
                                storage_type_, typed_ptr, 0,
                                numitems + val_idx));

  // If there's a length-component, store it at the appropriate index too
  if (len != nullptr) {
    codegen->CreateStore(len, codegen->CreateConstInBoundsGEP2_32(
                                  storage_type_, typed_ptr, 0,
                                  numitems + len_idx));
  }

  codegen->CreateStore(null, codegen->CreateConstInBoundsGEP2_32(
      storage_type_, typed_ptr, 0, index));

}

}  // namespace codegen
}  // namespace peloton
