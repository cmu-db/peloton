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

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "type/type.h"

#include <vector>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// This class enables the compact storage of a given set of types into a
// contiguous storage space. To use this, add the types of the elements
// you'd like to store using calls to Add(...).  When you're done, call
// Finalize(...).  At this point, the class can be used to pack and unpack
// values to and from a given storage space.
//
// TODO: We don't actually handle NULL values, so not really compact.
//===----------------------------------------------------------------------===//
class CompactStorage {
 public:
  // Constructor
  CompactStorage() : storage_type_(nullptr) {}

  // Add a type that is stored in the storage area
  void Add(type::Type::TypeId type) { types_.push_back(type); }

  // Finalize
  llvm::Type *Finalize(CodeGen &codegen);

  // Pack the given values into the provided storage area
  llvm::Value *Pack(CodeGen &codegen, llvm::Value *ptr,
                    const std::vector<codegen::Value> &vals) const;

  // Unpack the values stored at the given storage area into the provided vector
  llvm::Value *Unpack(CodeGen &codegen, llvm::Value *ptr,
                      std::vector<codegen::Value> &vals) const;

  uint64_t MaxPackedSize() const;

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

  // The constructed finalized type;
  llvm::Type *storage_type_;
};

}  // namespace codegen
}  // namespace peloton