//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table.h
//
// Identification: src/include/codegen/cc_hash_table.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/hash_table.h"
#include "codegen/value.h"

#include <functional>

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Our interface to the collision-chain hash-table data structure.
//
// DEPRECATED
//===----------------------------------------------------------------------===//
class CCHashTable : public HashTable {
 public:
  // Constructor
  CCHashTable();
  CCHashTable(CodeGen &codegen, const std::vector<type::Type> &key_type);

  // Initialize the hash-table instance
  void Init(CodeGen &codegen, llvm::Value *ht_ptr) const override;

  // Generate code to handle the insertion of a new tuple
  void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                     const std::vector<codegen::Value> &key,
                     HashTable::ProbeCallback &probe_callback,
                     HashTable::InsertCallback &insert_callback) const override;

  // Insert a new entry into the hash table with the given keys, but don't
  // perform any key matching or merging
  void Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
              const std::vector<codegen::Value> &keys,
              HashTable::InsertCallback &callback) const override;

  // Generate code to iterate over the entire hash table
  void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
               HashTable::IterateCallback &callback) const override;

  void VectorizedIterate(CodeGen &codegen, llvm::Value *ht_ptr,
                         Vector &selection_vector,
                         VectorizedIterateCallback &callback) const override;

  // Generate code that iterates all the matches
  void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
               const std::vector<codegen::Value> &key,
               HashTable::IterateCallback &callback) const override;

  // Destroy/cleanup the hash table
  void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const override;

 private:
  // The storage strategy we use to store the lookup keys inside every HashEntry
  CompactStorage key_storage_;
};

}  // namespace codegen
}  // namespace peloton