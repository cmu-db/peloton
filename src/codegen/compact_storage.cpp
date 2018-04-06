//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.cpp
//
// Identification: src/codegen/compact_storage.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compact_storage.h"

#include "codegen/type/sql_type.h"

namespace peloton {
namespace codegen {

// TODO: Only load/store values if it's not NULL

namespace {

class BitmapWriter {
 public:
  BitmapWriter(CodeGen &codegen, llvm::Value *bitmap_ptr, uint32_t num_bits)
      : bitmap_ptr_(bitmap_ptr) {
    if (bitmap_ptr_->getType() != codegen.CharPtrType()) {
      bitmap_ptr_ =
          codegen->CreateBitOrPointerCast(bitmap_ptr_, codegen.CharPtrType());
    }
    bytes_.resize((num_bits + 7) >> 3, nullptr);
  }

  void SetBit(CodeGen &codegen, uint32_t bit_idx, llvm::Value *bit_val) {
    PELOTON_ASSERT(bit_val->getType() == codegen.BoolType());
    // Cast to byte, left shift into position
    auto *byte_val = codegen->CreateZExt(bit_val, codegen.ByteType());
    byte_val = codegen->CreateShl(byte_val, bit_idx & 7);

    // Store in bytes
    uint32_t byte_pos = bit_idx >> 3;
    bytes_[byte_pos] = (bytes_[byte_pos] == nullptr)
                           ? byte_val
                           : codegen->CreateOr(bytes_[byte_pos], byte_val);
  }

  void Write(CodeGen &codegen) const {
    for (uint32_t idx = 0; idx < bytes_.size(); idx++) {
      llvm::Value *addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.ByteType(), bitmap_ptr_, idx);
      if (bytes_[idx] != nullptr) {
        codegen->CreateStore(bytes_[idx], addr);
      } else {
        codegen->CreateStore(codegen.Const8(0), addr);
      }
    }
  }

 private:
  // A pointer to the bitmap
  llvm::Value *bitmap_ptr_;

  // The bytes that compose the bitmap
  std::vector<llvm::Value *> bytes_;
};

class BitmapReader {
 public:
  BitmapReader(CodeGen &codegen, llvm::Value *bitmap_ptr, uint32_t num_bits)
      : bitmap_ptr_(bitmap_ptr) {
    if (bitmap_ptr_->getType() != codegen.CharPtrType()) {
      bitmap_ptr_ =
          codegen->CreateBitOrPointerCast(bitmap_ptr_, codegen.CharPtrType());
    }
    bytes_.resize((num_bits + 7) >> 3, nullptr);
  }

  llvm::Value *GetBit(CodeGen &codegen, uint32_t bit_idx) {
    uint32_t byte_pos = bit_idx >> 3;
    if (bytes_[byte_pos] == nullptr) {
      // Load the byte
      auto *byte_addr = codegen->CreateConstInBoundsGEP1_32(
          codegen.ByteType(), bitmap_ptr_, byte_pos);
      bytes_[byte_pos] = codegen->CreateLoad(byte_addr);
    }
    // Pull out only the bit we want
    auto *mask = codegen.Const8(1 << (bit_idx & 7));
    auto *masked_byte = codegen->CreateAnd(bytes_[byte_pos], mask);

    // Return if it equals 1
    return codegen->CreateICmpNE(masked_byte, codegen.Const8(0));
  }

 private:
  // A pointer to the bitmap
  llvm::Value *bitmap_ptr_;

  // The bytes that compose the bitmap
  std::vector<llvm::Value *> bytes_;
};

}  // anonymous namespace

//===----------------------------------------------------------------------===//
// Setup
//===----------------------------------------------------------------------===//
llvm::Type *CompactStorage::Setup(CodeGen &codegen,
                                  const std::vector<type::Type> &types) {
  // Return the constructed type if the compact storage has already been set up
  if (storage_type_ != nullptr) {
    return storage_type_;
  }

  // Copy over the types for convenience
  schema_ = types;

  // Add tracking metadata for all data elements that will be stored
  for (uint32_t i = 0; i < schema_.size(); i++) {
    const auto &sql_type = schema_[i].GetSqlType();

    llvm::Type *val_type = nullptr;
    llvm::Type *len_type = nullptr;
    sql_type.GetTypeForMaterialization(codegen, val_type, len_type);

    // Create a slot metadata entry for the value
    // Note: The physical and logical index are the same for now. The physical
    //       index is modified after storage format optimization (later).
    storage_format_.push_back(
        EntryInfo{val_type, i, i, false, codegen.SizeOf(val_type)});

    // If there is a length component, add that too
    if (len_type != nullptr) {
      storage_format_.push_back(
          EntryInfo{len_type, i, i, true, codegen.SizeOf(len_type)});
    }
  }

  // Sort the entries by decreasing size. This minimizes storage overhead due to
  // padding (potentially) added by LLVM.
  // TODO: Does this help?
  std::sort(storage_format_.begin(), storage_format_.end(),
            [](const EntryInfo &left, const EntryInfo &right) {
              return right.num_bytes < left.num_bytes;
            });

  // Now we construct the LLVM type of this storage space. First comes bytes
  // to manage the null bitmap. Then all the data elements.
  std::vector<llvm::Type *> llvm_types;

  uint64_t num_null_bitmap_bytes = (schema_.size() + 7) / 8;
  for (uint32_t i = 0; i < num_null_bitmap_bytes; i++) {
    llvm_types.push_back(codegen.Int8Type());
  }

  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    llvm_types.push_back(storage_format_[i].type);

    // Update the physical index in the storage entry
    storage_format_[i].physical_index = num_null_bitmap_bytes + i;
  }

  // Construct the finalized types
  storage_type_ = llvm::StructType::get(codegen.GetContext(), llvm_types, true);
  storage_size_ = static_cast<uint32_t>(codegen.SizeOf(storage_type_));
  return storage_type_;
}

//===----------------------------------------------------------------------===//
// Stores the given values into the provided storage area
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::StoreValues(
    CodeGen &codegen, llvm::Value *area_start,
    const std::vector<codegen::Value> &to_store) const {
  PELOTON_ASSERT(storage_type_ != nullptr);
  PELOTON_ASSERT(to_store.size() == schema_.size());

  // Decompose the values we're storing into their raw value, length and
  // null-bit components
  const uint32_t nitems = static_cast<uint32_t>(schema_.size());

  std::vector<llvm::Value *> vals{nitems}, lengths{nitems}, nulls{nitems};

  for (uint32_t i = 0; i < nitems; i++) {
    to_store[i].ValuesForMaterialization(codegen, vals[i], lengths[i],
                                         nulls[i]);
  }

  // Cast the area pointer to our constructed type
  auto *typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());

  // The NULL bitmap
  BitmapWriter null_bitmap{codegen, area_start, nitems};

  // Fill in the actual values
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    const auto &entry_info = storage_format_[i];

    // Load the address where this entry's data is in the storage space
    llvm::Value *addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, entry_info.physical_index);

    // Load it
    if (entry_info.is_length) {
      codegen->CreateStore(lengths[entry_info.logical_index], addr);
    } else {
      codegen->CreateStore(vals[entry_info.logical_index], addr);

      // Update the bitmap
      null_bitmap.SetBit(codegen, entry_info.logical_index,
                         nulls[entry_info.logical_index]);
    }
  }

  // Write the NULL bitmap
  null_bitmap.Write(codegen);

  // Return a pointer into the space just after all the entries we just wrote
  return codegen->CreateConstInBoundsGEP1_32(codegen.ByteType(), area_start,
                                             storage_size_);
}

//===----------------------------------------------------------------------===//
// Load the values stored compactly at the provided storage area into the
// provided vector
//===----------------------------------------------------------------------===//
llvm::Value *CompactStorage::LoadValues(
    CodeGen &codegen, llvm::Value *area_start,
    std::vector<codegen::Value> &output) const {
  const uint32_t nitems = static_cast<uint32_t>(schema_.size());
  std::vector<llvm::Value *> vals{nitems}, lengths{nitems}, nulls{nitems};

  // The NULL bitmap
  BitmapReader null_bitmap{codegen, area_start, nitems};

  // Collect all the values in the provided storage space, separating the
  // values into either value components or length components
  auto *typed_ptr =
      codegen->CreateBitCast(area_start, storage_type_->getPointerTo());
  for (uint32_t i = 0; i < storage_format_.size(); i++) {
    const auto &entry_info = storage_format_[i];

    // Load the raw value
    llvm::Value *entry_addr = codegen->CreateConstInBoundsGEP2_32(
        storage_type_, typed_ptr, 0, entry_info.physical_index);
    llvm::Value *entry = codegen->CreateLoad(entry_addr);

    // Set the length or value component
    if (entry_info.is_length) {
      lengths[entry_info.logical_index] = entry;
    } else {
      vals[entry_info.logical_index] = entry;

      // Load the null-bit too
      nulls[entry_info.logical_index] =
          null_bitmap.GetBit(codegen, entry_info.logical_index);
    }
  }

  // Create the values
  output.resize(nitems);
  for (uint64_t i = 0; i < nitems; i++) {
    output[i] = codegen::Value::ValueFromMaterialization(schema_[i], vals[i],
                                                         lengths[i], nulls[i]);
  }

  // Return a pointer into the space just after all the entries we stored
  return codegen->CreateConstInBoundsGEP1_32(
      codegen.ByteType(),
      codegen->CreateBitCast(area_start, codegen.CharPtrType()), storage_size_);
}

}  // namespace codegen
}  // namespace peloton
