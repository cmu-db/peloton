//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table.h
//
// Identification: src/include/codegen/oa_hash_table.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/hash_table.h"
#include "codegen/value.h"
#include "codegen/vector.h"
#include "planner/attribute_info.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Our interface to the open-addressing hash-table data structure.
//===----------------------------------------------------------------------===//
class OAHashTable : public HashTable {
 public:
  // The default group prefetch distance
  static uint32_t kDefaultGroupPrefetchSize;

  // A global pointer for attribute hashes
  static const planner::AttributeInfo kHashAI;

  //===--------------------------------------------------------------------===//
  // Convenience class providing a random access interface over the hash-table
  //===--------------------------------------------------------------------===//
  class OAHashTableAccess : public HashTable::HashTableAccess {
   public:
    OAHashTableAccess(const OAHashTable &hash_table, llvm::Value *ht_ptr)
        : hash_table_(hash_table), ht_ptr_(ht_ptr) {}

    void ExtractBucketKeys(CodeGen &codegen, llvm::Value *index,
                           std::vector<codegen::Value> &keys) const override;

    llvm::Value *BucketValue(CodeGen &codegen,
                             llvm::Value *index) const override;

   private:
    // The hash table translator
    const OAHashTable &hash_table_;
    // The pointer to the OAHashTable* instance
    llvm::Value *ht_ptr_;
  };

 public:
  // Constructor
  OAHashTable();
  OAHashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
              uint64_t value_size);

  void Init(CodeGen &codegen, llvm::Value *ht_ptr) const override;

  llvm::Value *HashKey(CodeGen &codegen,
                       const std::vector<codegen::Value> &key) const;

  // Generate code to handle the insertion of a new tuple
  void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                     const std::vector<codegen::Value> &key,
                     HashTable::ProbeCallback &probe_callback,
                     HashTable::InsertCallback &insert_callback) const override;

  // Probe the hash table and insert a new slot if needed, returning both the
  // result and the data pointer
  ProbeResult ProbeOrInsert(
      CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
      const std::vector<codegen::Value> &key) const override;

  // Insert a new entry into the hash table with the given keys, not caring
  // about existing entries.
  void Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
              const std::vector<codegen::Value> &key,
              HashTable::InsertCallback &callback) const override;

  // Generate code to iterate over the entire hash table
  void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
               HashTable::IterateCallback &callback) const override;

  // Generate code to iterate over the entire hash table in vectorized fashion
  void VectorizedIterate(
      CodeGen &codegen, llvm::Value *ht_ptr, Vector &selection_vector,
      HashTable::VectorizedIterateCallback &callback) const override;

  // Generate code that iterates all the matches
  void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
               const std::vector<codegen::Value> &key,
               HashTable::IterateCallback &callback) const override;

  // An enum class indicating the type of prefetch (i.e., read or write)
  enum class PrefetchType : uint32_t { Read = 0, Write = 0 };

  // An enum class indicating the temporal locality of the prefetched address
  enum class Locality : uint32_t { None = 0, High = 1, Medium = 2, Low = 3 };

  // Prefetch the first bucket with the given hash value from the hash table
  void PrefetchBucket(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                      PrefetchType pf_type, Locality locality) const;

  // Destroy/cleanup the hash table whose address is stored in the given LLVM
  // register/value
  void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const override;

  // Return the size of the hash entry
  // If the hash entry is initialized properly then this is the actual
  // size of the hash entry which should consist of three parts:
  // struct HashEntry, key size, and value size
  uint64_t HashEntrySize() const { return hash_entry_size_; }

 private:
  struct HashTablePos {
    llvm::Value *entry_index;
    llvm::Value *entry_ptr;

    HashTablePos(llvm::Value *entry_index_, llvm::Value *entry_ptr_)
        : entry_index(entry_index_), entry_ptr(entry_ptr_) {}
  };

  ProbeResult TranslateProbing(CodeGen &codegen, llvm::Value *hash_table,
                               llvm::Value *hash,
                               const std::vector<codegen::Value> &key,
                               std::function<void(llvm::Value *)> key_found,
                               std::function<void(llvm::Value *)> key_not_found,
                               bool process_value, bool process_only_one_value,
                               bool create_key_if_missing,
                               bool return_probe_result) const;

  llvm::Value *LoadHashTableField(CodeGen &codegen, llvm::Value *hash_table,
                                  uint32_t field_id) const;

  llvm::Value *LoadHashEntryField(CodeGen &codegen, llvm::Value *entry_ptr,
                                  uint32_t offset, uint32_t field_id) const;

  llvm::Value *PtrToInt(CodeGen &codegen, llvm::Value *ptr) const;

  llvm::Value *AdvancePointer(CodeGen &codegen, llvm::Value *ptr,
                              uint64_t delta) const;

  llvm::Value *AdvancePointer(CodeGen &codegen, llvm::Value *ptr,
                              llvm::Value *delta) const;

  HashTablePos GetNextEntry(CodeGen &codegen, llvm::Value *hash_table,
                            llvm::Value *entry_ptr, llvm::Value *index) const;

  llvm::Value *GetEntry(CodeGen &codegen, llvm::Value *hash_table,
                        llvm::Value *index) const;

  HashTablePos GetEntryByHash(CodeGen &codegen, llvm::Value *hash_table,
                              llvm::Value *hash_value) const;

  llvm::Value *GetKeyValueList(CodeGen &codegen, llvm::Value *entry_ptr) const;

  llvm::Value *IsPtrEqualTo(CodeGen &codegen, llvm::Value *value,
                            uint64_t target) const;

  llvm::Value *IsPtrUnEqualTo(CodeGen &codegen, llvm::Value *value,
                              uint64_t target) const;

  llvm::Value *GetKeyPtr(CodeGen &codegen, llvm::Value *entry_ptr) const;

  std::pair<llvm::Value *, llvm::Value *> GetDataCountAndPointer(
      CodeGen &codegen, llvm::Value *kv_p, llvm::Value *after_key_p) const;

 private:
  // The storage format we use to store the keys inside HashEntrys
  CompactStorage key_storage_;

  // The size of the hash entry. This value will be made an LLVM-compile-time
  // constant so that multiplications will likely be optimized away using shifts
  // and additions.
  uint64_t hash_entry_size_;

  // The size of value
  uint64_t value_size_;
};

}  // namespace codegen
}  // namespace peloton