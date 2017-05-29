//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updateable_storage.h
//
// Identification: src/include/codegen/updateable_storage.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A storage area where slots can be updated.
//===----------------------------------------------------------------------===//
class UpdateableStorage {
 public:
  // Constructor
  UpdateableStorage() : storage_size_(0), storage_type_(nullptr) {}

  // Add the given type to the storage format. We return the index that this
  // value can be found it (i.e., the index to pass into Get() to get the value)
  uint32_t AddType(type::Type::TypeId type);

  // Construct the final LLVM type given all the types that'll be stored
  llvm::Type *Finalize(CodeGen &codegen);

  // Get the value at a specific index into the storage area
  codegen::Value GetValueAt(CodeGen &codegen, llvm::Value *area_start,
                            uint64_t index) const;

  // Get the value at a specific index into the storage area
  void SetValueAt(CodeGen &codegen, llvm::Value *area_start, uint64_t index,
                  const codegen::Value &value) const;

  // Return the format of the storage area
  llvm::Type *GetStorageType() const { return storage_type_; }

  // Return the total bytes required by this storage format
  uint32_t GetStorageSize() const { return storage_size_; }

  // Return the number of elements this format is configured to handle
  uint32_t GetNumElements() const {
    return static_cast<uint32_t>(schema_.size());
  }

 private:
  // Find the position in the underlying storage where the item with the
  // provided index is.
  void FindStoragePositionFor(uint32_t item_index, int32_t &val_idx,
                              int32_t &len_idx) const;

 private:
  // The types we store in the storage area
  std::vector<type::Type::TypeId> schema_;

  // The physical storage format
  std::vector<CompactStorage::EntryInfo> storage_format_;

  // The total number of bytes needed for this format
  uint32_t storage_size_;

  // The finalized LLVM type that represents this storage area
  llvm::Type *storage_type_;
};

}  // namespace codegen
}  // namespace peloton