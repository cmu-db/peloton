//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updateable_storage.h
//
// Identification: src/include/codegen/updateable_storage.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

namespace lang {
class If;
}  // namespace lang

//===----------------------------------------------------------------------===//
// A storage area where slots can be updated.
//===----------------------------------------------------------------------===//
class UpdateableStorage {
 public:
  /// Constructor
  UpdateableStorage()
      : storage_size_(0), storage_type_(nullptr), null_bitmap_type_(nullptr) {}

  // Add the given type to the storage format. We return the index that this
  // value can be found it (i.e., the index to pass into Get() to get the value)
  uint32_t AddType(const type::Type &type);

  // Construct the final LLVM type given all the types that'll be stored
  llvm::Type *Finalize(CodeGen &codegen);

  // Forward declare - convenience class to handle NULL bitmaps.
  class NullBitmap;

  // Get the value at a specific index into the storage area, ignoring whether
  // the value is NULL or not
  codegen::Value GetValueSkipNull(CodeGen &codegen, llvm::Value *space,
                                  uint64_t index) const;

  // Like GetValueIgnoreNull(), but this also reads the NULL bitmap to determine
  // if the value is null.
  codegen::Value GetValue(CodeGen &codegen, llvm::Value *space, uint64_t index,
                          NullBitmap &null_bitmap) const;

  // Set the given value at the specific index in the storage area, ignoring to
  // update the bitmap
  void SetValueSkipNull(CodeGen &codegen, llvm::Value *space, uint64_t index,
                        const codegen::Value &value) const;

  // Like SetValueIgnoreNull(), but this also updates the NULL bitmap
  void SetValue(CodeGen &codegen, llvm::Value *space, uint64_t index,
                const codegen::Value &value, NullBitmap &null_bitmap) const;

  // Return the format of the storage area
  llvm::Type *GetStorageType() const { return storage_type_; }
  llvm::Type *GetNullBitmapType() const { return null_bitmap_type_; }

  // Return the total bytes required by this storage format
  uint32_t GetStorageSize() const { return storage_size_; }

  // Return the number of elements this format is configured to handle
  uint32_t GetNumElements() const {
    return static_cast<uint32_t>(schema_.size());
  }

 public:
  // Convenience class to handle NULL bitmaps.
  class NullBitmap {
   public:
    NullBitmap(CodeGen &codegen, const UpdateableStorage &storage,
               llvm::Value *storage_ptr);

    void InitAllNull(CodeGen &codegen);

    // Is the attribute at the provided index NULLable?
    bool IsNullable(uint32_t index) const;

    // Get the byte component where the given bit is
    llvm::Value *ByteFor(CodeGen &codegen, uint32_t index);

    // Is the value at the provided index null?
    llvm::Value *IsNull(CodeGen &codegen, uint32_t index);

    // Set the given bit to the provided value
    void SetNull(CodeGen &codegen, uint32_t index, llvm::Value *null_bit);

    void MergeValues(lang::If &if_clause, llvm::Value *before_if_value);

    // Write all the dirty byte components
    void WriteBack(CodeGen &codegen);

   private:
    // The storage format
    const UpdateableStorage &storage_;

    // The pointer to the bitmap
    llvm::Value *bitmap_ptr_;

    // The original and modified byte components of the bitmap.
    std::vector<llvm::Value *> bytes_;

    // Dirty bitmap
    std::vector<bool> dirty_;

    // The last byte component that was modified
    uint32_t active_byte_pos_;
  };

 private:
  // Find the position in the underlying storage where the item with the
  // provided index is.
  void FindStoragePositionFor(uint32_t item_index, int32_t &val_idx,
                              int32_t &len_idx) const;

 private:
  // The types we store in the storage area
  std::vector<type::Type> schema_;

  // Metadata about element types stored here
  std::vector<CompactStorage::EntryInfo> storage_format_;

  // The total number of bytes needed for this format
  uint32_t storage_size_;

  // The finalized LLVM type that represents this storage area
  llvm::Type *storage_type_;

  // The position in the final struct where the bitmap lives
  uint32_t null_bitmap_pos_;
  // The type of the bitmap
  llvm::Type *null_bitmap_type_;
};

}  // namespace codegen
}  // namespace peloton