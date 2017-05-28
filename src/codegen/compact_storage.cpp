//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.cpp
//
// Identification: src/codegen/compact_storage.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compact_storage.h"

#include "codegen/bitmap.h"
#include "codegen/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Setup
//===----------------------------------------------------------------------===//
llvm::Type *CompactStorage::Setup(
    CodeGen &codegen, const std::vector<type::Type::TypeId> &types) {
  // Return the constructed type if the compact storage has already been set up
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  // Copy over the types for convenience
  types_ = types;
  const auto num_items = types_.size();

  std::vector<llvm::Type *> llvm_types;

  // Put in null bytes at the front
  uint64_t num_null_bitmap_bytes = (num_items + 7) / 8;
  for (uint32_t i = 0; i < num_null_bitmap_bytes; i++) {
    llvm_types.push_back(codegen.Int8Type());
  }

  // Construct the storage for the values

  for (uint32_t i = 0; i < num_items; i++) {
    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, types_[i], val_type, len_type);

    // Create a slot metadata entry for the value
    uint32_t val_type_size = codegen.SizeOf(val_type);
    storage_format_.emplace_back(EntryInfo{val_type, i, false, val_type_size});

    // Add the LLVM type of the value into the structure type we're creating
    llvm_types.push_back(val_type);

    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the length
      uint32_t len_type_size = codegen.SizeOf(len_type);
      storage_format_.emplace_back(EntryInfo{len_type, i, true, len_type_size});

      // Add the LLVM type of the length into the structure type we're creating
      llvm_types.push_back(len_type);
    }
  }

  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = codegen.SizeOf(storage_type_);
  return storage_type_;
}

//===----------------------------------------------------------------------===//
// Stores the given values into the provided storage area
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::StoreValues(
    CodeGen &codegen, llvm::Value *ptr,
    const std::vector<codegen::Value> &to_store) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(to_store.size() == types_.size());

  // Decompose the values we're storing into their raw value, length and
  // null-bit components
  const uint32_t nitems = static_cast<uint32_t>(types_.size());

  std::vector<llvm::Value *> vals{nitems}, lengths{nitems}, nulls{nitems};

  for (uint32_t i = 0; i < nitems; i++) {
    to_store[i].ValuesForMaterialization(codegen, vals[i], lengths[i],
                                         nulls[i]);
  }

  // Cast the area pointer to our constructed type
  auto *typed_ptr = codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Fill in the NULL bitmap
  Bitmap null_bitmap{codegen, ptr, nitems};
  uint64_t num_null_components = null_bitmap.NumComponents();

  for (uint32_t i = 0; i < nitems; i++) {
    null_bitmap.SwitchBit(codegen, i, nulls[i]);
  }

  null_bitmap.WriteBack(codegen);

  // Fill in the actual values, if they're not NULL
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    llvm::Value *addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_null_components + i);
    const auto &entry_info = storage_format_[i];
    if (entry_info.is_length)
      codegen->CreateStore(lengths[entry_info.index], addr);
    else
      codegen->CreateStore(vals[entry_info.index], addr);
  }

  // Return a pointer into the space just after all the entries we just wrote
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), ptr,
                                             storage_size_);
}

//===----------------------------------------------------------------------===//
// Load the values stored compactly at the provided storage area into the
// provided vector
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::LoadValues(
    CodeGen &codegen, llvm::Value *ptr,
    std::vector<codegen::Value> &output) const {
  const uint32_t nitems = static_cast<uint32_t>(types_.size());
  std::vector<llvm::Value *> vals{nitems}, lengths{nitems}, nulls{nitems};

  Bitmap null_bitmap{codegen, ptr, nitems};
  uint64_t num_null_components = null_bitmap.NumComponents();

  // Collect all the values in the provided storage space, separating the values
  // into either value components or length components
  auto *typed_ptr = codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    const auto &entry_info = storage_format_[i];

    // Load the raw value
    llvm::Value *entry_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, num_null_components + i);
    llvm::Value *entry = codegen->CreateLoad(entry_addr);

    // Set the length or value component
    if (entry_info.is_length) {
      lengths[entry_info.index] = entry;
    } else {
      vals[entry_info.index] = entry;

      // Load the null-bit too
      nulls[entry_info.index] = null_bitmap.GetBit(codegen, entry_info.index);
    }
  }

  // Create the values
  output.resize(nitems);
  for (uint64_t i = 0; i < nitems; i++) {
    output[i] = codegen::Value::ValueFromMaterialization(types_[i], vals[i],
                                                         lengths[i], nulls[i]);
  }

  // Return a pointer into the space just after all the entries we stored
  return codegen->CreateConstInBoundsGEP1_32(
      codegen.ByteType(), codegen->CreateBitCast(ptr, codegen.CharPtrType()),
      storage_size_);
}

// Return the maximum possible bytes that this compact storage will need
uint64_t CompactStorage::MaxStorageSize() const { return storage_size_; }

}  // namespace codegen
}  // namespace peloton
