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

  // We keep no EntryInfo for each bit since it is waste of memory
  llvm::Type *null_bit = codegen.BoolType();
  for (uint32_t i = 0; i < num_items; i++) {
    llvm_types.push_back(null_bit);
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

  // Collect all the values and put them into values, lengths and nulls
  const auto numitems = types_.size();
  std::vector<llvm::Value *> vals(numitems), lengths(numitems), nulls(numitems);
  for (uint32_t i = 0; i < numitems; i++) {
    to_store[i].ValuesForMaterialization(vals[i], lengths[i], nulls[i]);

    // Giant hack while we fix-up nulls
    if (nulls[i] == nullptr) {
      nulls[i] = codegen::Value::SetNullValue(codegen, to_store[i]);
    }
  }

  // Cast the area pointer to our construct type, removing taking care of offset
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());

  // Store null bitmaps
  for (uint32_t i = 0; i < numitems; i++) {
    llvm::Value *null_addr =
        codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, i);
    codegen->CreateStore(nulls[i], null_addr);
  }
  auto numentries = storage_format_.size();
  for (uint32_t i = 0; i < numentries; i++) {
    llvm::Value *addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, numitems + i);
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
  const auto numitems = types_.size();
  std::vector<llvm::Value *> vals(numitems), lengths(numitems), nulls(numitems);

  // Collect all the values in the provided storage space, separating the values
  // into either value components or length components
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  const auto numentries = storage_format_.size();
  for (uint32_t i = 0; i < numentries; i++) {
    const auto &entry_info = storage_format_[i];
    llvm::Value *entry =
        codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
            storage_type_, typed_ptr, 0, numitems + i));
    if (entry_info.is_length) {
      lengths[entry_info.index] = entry;
    } else {
      vals[entry_info.index] = entry;
      llvm::Value *null =
          codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
              storage_type_, typed_ptr, 0, entry_info.index));
      nulls[entry_info.index] = null;
    }
  }

  // Create the values
  output.resize(numitems);
  for (uint64_t i = 0; i < numitems; i++) {
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
