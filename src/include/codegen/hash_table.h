//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.h
//
// Identification: src/include/codegen/hash_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/compact_storage.h"
#include "codegen/value.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

class HashTable {
 public:
  /**
   * This callback functor is used when probing the hash table for a given
   * key. It is invoked when a matching key-value pair is found in the hash
   * table.
   */
  struct ProbeCallback {
    /** Virtual destructor */
    virtual ~ProbeCallback() = default;

    /**
     * The primary callback method. This is invoked for each matching key-value
     * pair. The value parameter is a pointer into the entry to an opaque set of
     * bytes that are the value associated to the input key. It is up to the
     * caller to interpret the bytes.
     *
     * @param codegen The code generator instance
     * @param value A pointer to the value bytes
     */
    virtual void ProcessEntry(CodeGen &codegen, llvm::Value *value) const = 0;
  };

  /**
   * A callback used when inserting insert a new entry into the hash table. The
   * caller implements the StoreValue() method to perform the insertion.  The
   * argument provided to StoreValue() is the address where the contents can be
   * stored.  The size of this space is equal to the value returned by
   * GetValueSize().
   */
  struct InsertCallback {
    /** Virtual destructor */
    virtual ~InsertCallback() = default;

    // Called to store the value associated with the key used for insertion.
    virtual void StoreValue(CodeGen &codegen, llvm::Value *space) const = 0;

    // Called to determine the size of the payload the caller wants to store
    virtual llvm::Value *GetValueSize(CodeGen &codegen) const = 0;
  };

  // No-op InsertCallback
  struct NoOpInsertCallback : InsertCallback {
    void StoreValue(UNUSED_ATTRIBUTE CodeGen &codegen,
                    UNUSED_ATTRIBUTE llvm::Value *space) const override {}
    llvm::Value *GetValueSize(
        UNUSED_ATTRIBUTE CodeGen &codegen) const override {
      return nullptr;
    }
  };

  //===--------------------------------------------------------------------===//
  // This callback is used to iterate over all (or a subset) of the entries in
  // the hash table that match a provided key.  The ProcessEntry() method is
  // called for every match and we provide a pointer to the actual HashEntry,
  // and a vector of codegen::Value for every value stored in the HashEntry.
  //===--------------------------------------------------------------------===//
  struct IterateCallback {
    /** Virtual destructor */
    virtual ~IterateCallback() = default;

    // Callback to process an entry in the hash table. The key and opaque set of
    // bytes (representing the value space) are provided as arguments.
    virtual void ProcessEntry(CodeGen &codegen,
                              const std::vector<codegen::Value> &keys,
                              llvm::Value *values) const = 0;
  };

  class HashTableAccess;

  //===--------------------------------------------------------------------===//
  // This callback is used when performing a vectorized iteration over all
  // entries in the hash table that match a provided key. For each vector of
  // entries, ProcessEntries() is called indicating the range of entries in the
  // hash-table to cover, and a selection vector indicating which entries in
  // this range are occupied.
  //===--------------------------------------------------------------------===//
  struct VectorizedIterateCallback {
    /** Virtual destructor */
    virtual ~VectorizedIterateCallback() = default;

    // Process a vector of entries in this hash-table
    virtual void ProcessEntries(CodeGen &codegen, llvm::Value *start,
                                llvm::Value *end, Vector &selection_vector,
                                HashTableAccess &access) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // Convenience class proving a random access interface over the hash-table
  //===--------------------------------------------------------------------===//
  class HashTableAccess {
   public:
    /** Virtual destructor */
    virtual ~HashTableAccess() = default;

    // Extract the keys for the bucket at the given index
    virtual void ExtractBucketKeys(CodeGen &codegen, llvm::Value *index,
                                   std::vector<codegen::Value> &keys) const = 0;
    virtual llvm::Value *BucketValue(CodeGen &codegen,
                                     llvm::Value *index) const = 0;
  };

  //===--------------------------------------------------------------------===//
  // Return type for ProbeOrInsert
  //===--------------------------------------------------------------------===//
  struct ProbeResult {
    // Actual probe result (bool), if the key already exists in the hast table
    llvm::Value *key_exists;

    // Data pointer (u8*), either to the existing data or to the new empty entry
    llvm::Value *data_ptr;
  };

 public:
  // Constructor
  HashTable();
  HashTable(CodeGen &codegen, const std::vector<type::Type> &key_type,
            uint32_t value_size);

  // Destructor
  virtual ~HashTable() = default;

  // Initialize the hash-table instance
  virtual void Init(CodeGen &codegen, llvm::Value *ht_ptr) const;
  virtual void Init(CodeGen &codegen, llvm::Value *exec_ctx,
                    llvm::Value *ht_ptr) const;

  // Generate code to handle the insertion of a new tuple
  virtual void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                             llvm::Value *hash,
                             const std::vector<codegen::Value> &key,
                             ProbeCallback &probe_callback,
                             InsertCallback &insert_callback) const;

  // Probe the hash table and insert a new slot if needed, returning both the
  // result and the data pointer
  virtual ProbeResult ProbeOrInsert(
      CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
      const std::vector<codegen::Value> &key) const;

  // Insert a new entry into the hash table with the given keys, but don't
  // perform any key matching or merging
  virtual void Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                      const std::vector<codegen::Value> &keys,
                      InsertCallback &callback) const;

  void InsertLazy(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                  const std::vector<codegen::Value> &keys,
                  InsertCallback &callback) const;

  void BuildLazy(CodeGen &codegen, llvm::Value *ht_ptr) const;

  void ReserveLazy(CodeGen &codegen, llvm::Value *ht_ptr,
                   llvm::Value *thread_states) const;

  void MergeLazyUnfinished(CodeGen &codegen, llvm::Value *global_ht,
                           llvm::Value *local_ht) const;

  // Generate code to iterate over the entire hash table
  virtual void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                       IterateCallback &callback) const;

  // Generate code to iterate over the entire hash table in vectorized fashion
  virtual void VectorizedIterate(CodeGen &codegen, llvm::Value *ht_ptr,
                                 Vector &selection_vector,
                                 VectorizedIterateCallback &callback) const;

  // Generate code that iterates all the matches
  virtual void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                       const std::vector<codegen::Value> &key,
                       IterateCallback &callback) const;

  // Destroy/cleanup the hash table
  virtual void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const;

 private:
  uint32_t value_size_;

  // The storage strategy we use to store the lookup keys inside every HashEntry
  CompactStorage key_storage_;
};

}  // namespace codegen
}  // namespace peloton