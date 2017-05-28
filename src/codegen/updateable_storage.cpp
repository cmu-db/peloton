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

#include "codegen/bitmap.h"
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
  return static_cast<uint32_t>(types_.size() - 1);
}

llvm::Type *UpdateableStorage::Finalize(CodeGen &codegen) {
  // Return the constructed type if we've already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  // The LLVM type we're constructing
  std::vector<llvm::Type *> llvm_types;

  // First, we prepend the NULL bitmap at the front
  // Put in null bytes at the front
  uint64_t num_null_bitmap_bytes = (types_.size() + 7) / 8;
  for (uint32_t i = 0; i < num_null_bitmap_bytes; i++) {
    llvm_types.push_back(codegen.Int8Type());
  }

  // Now, add the actual types
  for (uint32_t i = 0; i < types_.size(); i++) {
    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, types_[i], val_type, len_type);

    // Create an entry for the value and add the type to the struct type we're
    // constructing
    uint32_t val_type_size = static_cast<uint32_t>(codegen.SizeOf(val_type));
    storage_format_.push_back(
        CompactStorage::EntryInfo{val_type, i, false, val_type_size});
    llvm_types.push_back(val_type);

    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the length and add to the struct
      uint32_t len_type_size = static_cast<uint32_t>(codegen.SizeOf(len_type));
      storage_format_.push_back(
          CompactStorage::EntryInfo{len_type, i, true, len_type_size});
      llvm_types.push_back(len_type);
    }
  }
  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = static_cast<uint32_t>(codegen.SizeOf(storage_type_));
  return storage_type_;
}

void UpdateableStorage::FindStoragePositionFor(uint32_t item_index,
                                               int32_t &val_idx,
                                               int32_t &len_idx) const {
  // TODO: This linear search isn't great ...
  val_idx = len_idx = -1;
  for (uint32_t store_idx = 0; store_idx < storage_format_.size();
       store_idx++) {
    if (storage_format_[store_idx].index == item_index) {
      if (storage_format_[store_idx].is_length) {
        len_idx = store_idx;
      } else {
        val_idx = store_idx;
      }
    }
  }
  PL_ASSERT(val_idx >= 0);
}

// Get the value at a specific index into the storage area
codegen::Value UpdateableStorage::GetValueAt(CodeGen &codegen, llvm::Value *ptr,
                                             uint64_t index) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(index < types_.size());

  int32_t val_idx, len_idx;
  FindStoragePositionFor(index, val_idx, len_idx);

  Bitmap null_bitmap{codegen, ptr, static_cast<uint32_t>(types_.size())};
  uint64_t num_null_components = null_bitmap.NumComponents();

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Load the value
  llvm::Value *val_addr = codegen->CreateConstInBoundsGEP2_32(
      storage_type_, typed_ptr, 0, num_null_components + val_idx);
  llvm::Value *val = codegen->CreateLoad(val_addr);

  // If there is a length-component for this entry, load it too
  llvm::Value *len = nullptr;
  if (len_idx > 0) {
    llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_null_components + len_idx);
    len = codegen->CreateLoad(len_addr);
  }

  // Load the null-indication bit
  llvm::Value *null = null_bitmap.GetBit(codegen, index);

  // Done
  return codegen::Value{types_[index], val, len, null};
}

// Get the value at a specific index into the storage area
void UpdateableStorage::SetValueAt(CodeGen &codegen, llvm::Value *ptr,
                                   uint64_t index,
                                   const codegen::Value &value) const {
  llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
  value.ValuesForMaterialization(codegen, val, len, null);

  int32_t val_idx, len_idx;
  FindStoragePositionFor(index, val_idx, len_idx);

  PL_ASSERT(value.GetValue()->getType() == storage_format_[val_idx].type);

  Bitmap null_bitmap{codegen, ptr, static_cast<uint32_t>(types_.size())};
  uint64_t num_null_components = null_bitmap.NumComponents();

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Store the value and len at the appropriate slot
  llvm::Value *val_addr = codegen->CreateConstInBoundsGEP2_32(
      storage_type_, typed_ptr, 0, num_null_components + val_idx);
  codegen->CreateStore(val, val_addr);

  // If there's a length-component, store it at the appropriate index too
  if (len != nullptr) {
    llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_null_components + len_idx);
    codegen->CreateStore(len, len_addr);
  }

  // Write-back null-bit
  null_bitmap.SwitchBit(codegen, index, null);
  null_bitmap.WriteBack(codegen);
}

}  // namespace codegen
}  // namespace peloton