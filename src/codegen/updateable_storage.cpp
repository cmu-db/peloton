//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updateable_storage.cpp
//
// Identification: src/codegen/updateable_storage.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updateable_storage.h"

#include "ips4o/ips4o.hpp"

#include "codegen/lang/if.h"
#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Add the given type to the storage format. We return the index that this value
// can be found it (i.e., which index to pass into Get() to get the value)
//===----------------------------------------------------------------------===//
uint32_t UpdateableStorage::AddType(const type::Type &type) {
  PELOTON_ASSERT(storage_type_ == nullptr);
  schema_.push_back(type);
  return static_cast<uint32_t>(schema_.size() - 1);
}

llvm::Type *UpdateableStorage::Finalize(CodeGen &codegen) {
  // Return the constructed type if we've already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  // Add tracking metadata for all data elements that will be stored
  for (uint32_t i = 0; i < schema_.size(); i++) {
    const auto &sql_type = schema_[i].GetSqlType();

    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    sql_type.GetTypeForMaterialization(codegen, val_type, len_type);

    // Create a slot metadata entry for the value
    // Note: The physical and logical index are the same for now. The physical
    //       index is modified after storage format optimization (later).
    storage_format_.emplace_back(
        CompactStorage::EntryInfo{.type = val_type,
                                  .physical_index = i,
                                  .logical_index = i,
                                  .is_length = false,
                                  .num_bytes = codegen.SizeOf(val_type)});

    // If there is a length component, add that too
    if (len_type != nullptr) {
      storage_format_.emplace_back(
          CompactStorage::EntryInfo{.type = len_type,
                                    .physical_index = i,
                                    .logical_index = i,
                                    .is_length = true,
                                    .num_bytes = codegen.SizeOf(len_type)});
    }
  }

  // Sort the entries by decreasing size
  ips4o::sort(storage_format_.begin(), storage_format_.end(),
              [](const CompactStorage::EntryInfo &left,
                 const CompactStorage::EntryInfo &right) {
                return right.num_bytes < left.num_bytes;
              });

  // Now we construct the LLVM type of this storage space
  std::vector<llvm::Type *> llvm_types;
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    llvm_types.push_back(storage_format_[i].type);
    storage_format_[i].physical_index = i;
  }

  // If we need a null-bitmap, add it at the end
  auto num_null_bytes = static_cast<uint32_t>((schema_.size() + 7) >> 3);
  if (num_null_bytes > 0) {
    null_bitmap_pos_ = static_cast<uint32_t>(llvm_types.size());
    null_bitmap_type_ = codegen.ArrayType(codegen.ByteType(), num_null_bytes);
    llvm_types.push_back(null_bitmap_type_);
  }

  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = static_cast<uint32_t>(codegen.SizeOf(storage_type_));
  return storage_type_;
}

// NOTE: We take signed integer values for the value and index position because
// we use -1 for errors.
void UpdateableStorage::FindStoragePositionFor(uint32_t item_index,
                                               int32_t &val_idx,
                                               int32_t &len_idx) const {
  // TODO: This linear search isn't great ...
  val_idx = len_idx = -1;
  for (const auto &entry_info : storage_format_) {
    if (entry_info.logical_index == item_index) {
      if (entry_info.is_length) {
        PELOTON_ASSERT(len_idx == -1);
        len_idx = entry_info.physical_index;
      } else {
        PELOTON_ASSERT(val_idx == -1);
        val_idx = entry_info.physical_index;
      }
    }
  }
  PELOTON_ASSERT(val_idx >= 0);
}

// Get the value at a specific index into the storage area
codegen::Value UpdateableStorage::GetValueSkipNull(CodeGen &codegen,
                                                   llvm::Value *space,
                                                   uint64_t index) const {
  PELOTON_ASSERT(storage_type_ != nullptr);
  PELOTON_ASSERT(index < schema_.size());

  // Get the physical position in the storage space where the data is located
  int32_t val_idx, len_idx;
  FindStoragePositionFor(index, val_idx, len_idx);

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(space, storage_type_->getPointerTo());

  // Load the value
  llvm::Value *val_addr =
      codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, val_idx);
  llvm::Value *val = codegen->CreateLoad(val_addr);

  // If there is a length-component for this entry, load it too
  llvm::Value *len = nullptr;
  if (len_idx > 0) {
    llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, len_idx);
    len = codegen->CreateLoad(len_addr);
  }

  // Done
  auto type = schema_[index].AsNonNullable();
  return codegen::Value{type, val, len, nullptr};
}

codegen::Value UpdateableStorage::GetValue(
    CodeGen &codegen, llvm::Value *space, uint64_t index,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  // If the index isn't NULL-able, skip the check
  if (!null_bitmap.IsNullable(index)) {
    return GetValueSkipNull(codegen, space, index);
  }

  codegen::Value null_val, read_val;

  lang::If val_is_null(codegen, null_bitmap.IsNull(codegen, index));
  {
    // If the index has its null-bit set, return NULL
    const auto &type = schema_[index];
    null_val = type.GetSqlType().GetNullValue(codegen);
  }
  val_is_null.ElseBlock();
  {
    // If the index doesn't have its null-bit set, read from storage
    read_val = GetValueSkipNull(codegen, space, index);
  }
  val_is_null.EndIf();

  // Merge the values
  return val_is_null.BuildPHI(null_val, read_val);
}

// Get the value at a specific index into the storage area
void UpdateableStorage::SetValueSkipNull(CodeGen &codegen, llvm::Value *space,
                                         uint64_t index,
                                         const codegen::Value &value) const {
  llvm::Value *val = nullptr, *len = nullptr, *null = nullptr;
  value.ValuesForMaterialization(codegen, val, len, null);

  // Get the physical position in the storage space where the data is located
  int32_t val_idx, len_idx;
  FindStoragePositionFor(index, val_idx, len_idx);

  llvm::Value *typed_ptr =
      codegen->CreateBitCast(space, storage_type_->getPointerTo());

  // Store the value at the appropriate slot
  llvm::Value *val_addr =
      codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, val_idx);
  codegen->CreateStore(val, val_addr);

  // If there's a length-component, store it at the appropriate slot too
  if (len != nullptr) {
    llvm::Value *len_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, len_idx);
    codegen->CreateStore(len, len_addr);
  }
}

void UpdateableStorage::SetValue(
    CodeGen &codegen, llvm::Value *space, uint64_t index,
    const codegen::Value &value,
    UpdateableStorage::NullBitmap &null_bitmap) const {
  // If the index isn't NULL-able, skip storing the NULL bit
  if (!null_bitmap.IsNullable(index)) {
    SetValueSkipNull(codegen, space, index, value);
    return;
  }

  PELOTON_ASSERT(null_bitmap.IsNullable(index));

  // Set the NULL bit
  llvm::Value *null = value.IsNull(codegen);
  null_bitmap.SetNull(codegen, index, null);

  lang::If val_not_null(codegen, codegen->CreateNot(null));
  {
    // If the value isn't NULL, write it into storage
    SetValueSkipNull(codegen, space, index, value);
  }
  val_not_null.EndIf();
}

////////////////////////////////////////////////////////////////////////////////
///
/// Null Bitmap
///
////////////////////////////////////////////////////////////////////////////////

UpdateableStorage::NullBitmap::NullBitmap(CodeGen &codegen,
                                          const UpdateableStorage &storage,
                                          llvm::Value *storage_ptr)
    : storage_(storage), bitmap_ptr_(storage_ptr) {
  auto *storage_type = storage.GetStorageType();
  if (storage.GetNullBitmapType() != nullptr) {
    // Cast the pointer to the constructed storage type
    auto *typed_ptr = codegen->CreateBitOrPointerCast(
        storage_ptr, storage_type->getPointerTo());

    // Get the pointer to the bitmap array
    auto *bitmap_arr = codegen->CreateConstInBoundsGEP2_32(
        storage_type, typed_ptr, 0, storage.null_bitmap_pos_);

    // Index into the first element, treating it as a char *
    bitmap_ptr_ = codegen->CreateConstInBoundsGEP2_32(
        storage.GetNullBitmapType(), bitmap_arr, 0, 0);
  }
  uint32_t num_bytes = (storage_.GetNumElements() + 7) >> 3;
  bytes_.resize(num_bytes, nullptr);
  dirty_.resize(num_bytes, false);
}

void UpdateableStorage::NullBitmap::InitAllNull(CodeGen &codegen) {
  for (uint32_t byte_pos = 0; byte_pos < bytes_.size(); byte_pos++) {
    bytes_[byte_pos] = codegen.Const8(~0);
  }
  std::fill(dirty_.begin(), dirty_.end(), true);
}

bool UpdateableStorage::NullBitmap::IsNullable(uint32_t index) const {
  const auto &type = storage_.schema_[index];
  return type.nullable;
}

llvm::Value *UpdateableStorage::NullBitmap::ByteFor(CodeGen &codegen,
                                                    uint32_t index) {
  active_byte_pos_ = index >> 3;

  // Get the byte the caller wants
  if (bytes_[active_byte_pos_] == nullptr) {
    // Load it
    llvm::Value *byte_addr = codegen->CreateConstInBoundsGEP1_32(
        codegen.ByteType(), bitmap_ptr_, active_byte_pos_);
    bytes_[active_byte_pos_] = codegen->CreateLoad(byte_addr);
  }
  return bytes_[active_byte_pos_];
}

llvm::Value *UpdateableStorage::NullBitmap::IsNull(CodeGen &codegen,
                                                   uint32_t index) {
  llvm::Value *mask = codegen.Const8(1 << (index & 7));
  llvm::Value *masked = codegen->CreateAnd(ByteFor(codegen, index), mask);
  return codegen->CreateICmpNE(masked, codegen.Const8(0));
}

void UpdateableStorage::NullBitmap::SetNull(CodeGen &codegen, uint32_t index,
                                            llvm::Value *null_bit) {
  PELOTON_ASSERT(null_bit->getType() == codegen.BoolType());

  uint32_t byte_pos = index >> 3;

  // The current byte value
  llvm::Value *byte_val = ByteFor(codegen, index);

  llvm::Value *mask = codegen.Const8(1 << (index & 7));

  // If we know the bit is a compile-time constant, generate specialized code
  if (auto *const_int = llvm::dyn_cast<llvm::ConstantInt>(null_bit)) {
    if (const_int->isOne()) {
      bytes_[byte_pos] = codegen->CreateOr(byte_val, mask);
    } else {
      bytes_[byte_pos] = codegen->CreateAnd(byte_val, codegen->CreateNot(mask));
    }
  } else {
    // The null-bit is not a compile-time constant.

    llvm::Value *cleared =
        codegen->CreateAnd(byte_val, codegen->CreateNot(mask));
    llvm::Value *val = codegen->CreateShl(
        codegen->CreateZExt(null_bit, codegen.ByteType()), (index & 7));
    bytes_[byte_pos] = codegen->CreateOr(cleared, val);
  }

  // Mark dirty
  dirty_[byte_pos] = true;
}

void UpdateableStorage::NullBitmap::MergeValues(lang::If &if_clause,
                                                llvm::Value *before_if_value) {
  PELOTON_ASSERT(bytes_[active_byte_pos_] != nullptr);
  bytes_[active_byte_pos_] =
      if_clause.BuildPHI(bytes_[active_byte_pos_], before_if_value);
}

void UpdateableStorage::NullBitmap::WriteBack(CodeGen &codegen) {
  for (uint32_t byte_pos = 0; byte_pos < bytes_.size(); byte_pos++) {
    if (dirty_[byte_pos]) {
      llvm::Value *byte_addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.ByteType(), bitmap_ptr_, byte_pos);
      codegen->CreateStore(bytes_[byte_pos], byte_addr);
    }
  }
  std::fill(bytes_.begin(), bytes_.end(), nullptr);
  std::fill(dirty_.begin(), dirty_.end(), false);
}

}  // namespace codegen
}  // namespace peloton