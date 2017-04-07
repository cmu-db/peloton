//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compact_storage.h
//
// Identification: src/include/codegen/compact_storage.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class enables the compact storage of a given set of types into a
// contiguous storage space. To use this, call Setup(...) with the types of the
// elements you'd like to store. The class can then be used to store and
// retrieve values to and from a given storage space.
//
// TODO: We don't actually handle NULL values, so not really compact.
//===----------------------------------------------------------------------===//
class CompactStorage {
 public:
  // Constructor
  CompactStorage() : storage_type_(nullptr) {}

  // Setup this storage to store the given types (in the specified order)
  llvm::Type *Setup(CodeGen &codegen,
                    const std::vector<type::Type::TypeId> &types);

  // Store the given values into the provided storage area
  llvm::Value *StoreValues(CodeGen &codegen, llvm::Value *ptr,
                           const std::vector<codegen::Value> &vals) const;

  // Load the values stored at the given storage area into the provided
  // vector
  llvm::Value *LoadValues(CodeGen &codegen, llvm::Value *ptr,
                          std::vector<codegen::Value> &vals) const;

  // Get the maximum number of bytes this storage format will need
  uint64_t MaxStorageSize() const;

 private:
  friend class UpdateableStorage;

  //===--------------------------------------------------------------------===//
  // Metadata about each entry this storage is configured with
  //===--------------------------------------------------------------------===//
  struct EntryInfo {
    // The LLVM type of this entry
    llvm::Type *type;

    // The index in the format that this entry is associated with
    uint32_t index;

    // Indicates whether this is the length component of a variable length entry
    bool is_length;

    // The size in bytes this entry occupies
    uint32_t nbytes;
  };

 private:
  // The SQL types we store
  std::vector<type::Type::TypeId> types_;

  // The schema of the storage
  std::vector<CompactStorage::EntryInfo> storage_format_;

  // The constructed finalized type
  llvm::Type *storage_type_;

  // The size of the constructed finalized type
  uint32_t storage_size_;
};

}  // namespace codegen
}  // namespace peloton
