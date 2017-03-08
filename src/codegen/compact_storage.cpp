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

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Finalize
//===----------------------------------------------------------------------===//
llvm::Type *CompactStorage::Finalize(CodeGen &codegen) {
  // Return the constructed type if we've already been finalized
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  std::vector<llvm::Type *> llvm_types;
  for (uint32_t i = 0; i < types_.size(); i++) {
    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    codegen::Value::TypeForMaterialization(codegen, types_[i], val_type,
                                           len_type);
    // Create an entry for the value and add the type to the struct type we're
    // constructing
    uint32_t val_type_size = codegen.SizeOf(val_type);
    storage_format_.emplace_back(EntryInfo{val_type, i, false, val_type_size});
    llvm_types.push_back(val_type);
    // If there is a length component, add that too
    if (len_type != nullptr) {
      // Create an entry for the value and add the type to the struct type we're
      // constructing
      uint32_t len_type_size = codegen.SizeOf(len_type);
      storage_format_.emplace_back(EntryInfo{len_type, i, true, len_type_size});
      llvm_types.push_back(len_type);
    }
  }
  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  return storage_type_;
}

//===----------------------------------------------------------------------===//
// Pack the given values into the provided storage area
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::Pack(
    CodeGen &codegen, llvm::Value *ptr,
    const std::vector<codegen::Value> &to_pack) const {
  PL_ASSERT(storage_type_ != nullptr);
  PL_ASSERT(to_pack.size() == types_.size());

  std::vector<llvm::Value *> vals, lengths;
  vals.resize(types_.size());
  lengths.resize(types_.size());

  // Collect all the values, separating into the values into the either the
  // value component of the length component
  for (uint32_t i = 0; i < vals.size(); i++) {
    llvm::Value *val = nullptr;
    llvm::Value *len = nullptr;
    to_pack[i].ValuesForMaterialization(codegen, val, len);

    vals[i] = val;
    lengths[i] = len;
  }

  // Cast the storage space to our construct type so we don't have to worry
  // about offsets
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    llvm::Value *addr =
        codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, i);
    const auto &entry_info = storage_format_[i];
    codegen->CreateStore(entry_info.is_length ? lengths[entry_info.index]
                                              : vals[entry_info.index],
                         addr);
  }

  // Return a pointer into the space just after all the entries we've packed
  uint32_t storage_size = codegen.SizeOf(storage_type_);
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), ptr,
                                             storage_size);
}

//===----------------------------------------------------------------------===//
// Unpack the values stored compactly at the provided storage area into the
// provided vector
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::Unpack(CodeGen &codegen, llvm::Value *ptr,
                                    std::vector<codegen::Value> &output) const {
  std::vector<llvm::Value *> vals, lengths;
  vals.resize(types_.size());
  lengths.resize(types_.size());

  // Collect all the values in the provided storage space, separating the values
  // into either value components or length components
  llvm::Value *typed_ptr =
      codegen->CreateBitCast(ptr, storage_type_->getPointerTo());
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    const auto &entry_info = storage_format_[i];
    llvm::Value *entry = codegen->CreateLoad(
        codegen->CreateConstInBoundsGEP2_32(storage_type_, typed_ptr, 0, i));
    if (entry_info.is_length) {
      lengths[entry_info.index] = entry;
    } else {
      vals[entry_info.index] = entry;
    }
  }

  // Create the values
  output.resize(types_.size());
  for (uint64_t i = 0; i < types_.size(); i++) {
    output[i] = codegen::Value::ValueFromMaterialization(codegen, types_[i],
                                                         vals[i], lengths[i]);
  }

  // Return a pointer into the space just after all the entries we've packed
  uint32_t storage_size = codegen.SizeOf(storage_type_);
  return codegen->CreateConstInBoundsGEP1_32(
      codegen.ByteType(), codegen->CreateBitCast(ptr, codegen.CharPtrType()),
      storage_size);
}

// Return the maximum possible bytes that this compact storage will need
uint64_t CompactStorage::MaxPackedSize() const {
  uint32_t total_size = 0;
  for (const auto &entry : storage_format_) {
    total_size += entry.nbytes;
  }
  return total_size;
}

}  // namespace codegen
}  // namespace peloton