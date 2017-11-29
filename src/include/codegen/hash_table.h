//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.h
//
// Identification: src/include/codegen/hash_table.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/value.h"
#include "codegen/vector.h"

namespace peloton {
namespace codegen {

class HashTable {
 public:
  //===--------------------------------------------------------------------===//
  // A callback used when probing the hash table with a given key.  The callback
  // is only invoked when the value for the given key is successfully found.  If
  // multiple values for the key are found, an invocation occurs for each
  // key-value pair.
  //===--------------------------------------------------------------------===//
  struct ProbeCallback {
    // Destructor
    virtual ~ProbeCallback() {}

    // The callback method.  The "value" parameter is a pointer to an opaque set
    // of bytes that are associated with the key that was probed.  It is up to
    // the caller to interpret the meaning of the bytes.
    virtual void ProcessEntry(CodeGen &codegen, llvm::Value *value) const = 0;
  };

  // No-op ProbeCallback
  struct NoOpProbeCallback : ProbeCallback {
    void ProcessEntry(UNUSED_ATTRIBUTE CodeGen &codegen,
                      UNUSED_ATTRIBUTE llvm::Value *value) const override {}
  };

  //===--------------------------------------------------------------------===//
  // A callback used when inserting insert a new entry into the hash table. The
  // caller implements the StoreValue() method to perform the insertion.  The
  // argument provided to StoreValue() is the address where the contents can be
  // stored.  The size of this space is equal to the value returned by
  // GetValueSize().
  //===--------------------------------------------------------------------===//
  struct InsertCallback {
    // Destructor
    virtual ~InsertCallback() {}

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
    // Destructor
    virtual ~IterateCallback() {}

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
    // Destructor
    virtual ~VectorizedIterateCallback() {}

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
    // Destructor
    virtual ~HashTableAccess() {}

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
    // Actual probe result (bool), if the key alredy exists in the hast table
    llvm::Value *key_exists;

    // Data pointer (u8*), either to the existing data or to the new empty entry
    llvm::Value *data_ptr;
  };

  // Destructor
  virtual ~HashTable() {}

  // Initialize the hash-table instance
  virtual void Init(CodeGen &codegen, llvm::Value *ht_ptr) const = 0;

  // Generate code to handle the insertion of a new tuple
  virtual void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                             llvm::Value *hash,
                             const std::vector<codegen::Value> &key,
                             ProbeCallback &probe_callback,
                             InsertCallback &insert_callback) const = 0;

  // Probe the hash table and insert a new slot if needed, returning both the
  // result and the data pointer
  virtual ProbeResult ProbeOrInsert(
      CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
      const std::vector<codegen::Value> &key) const = 0;

  // Insert a new entry into the hash table with the given keys, but don't
  // perform any key matching or merging
  virtual void Insert(CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
                      const std::vector<codegen::Value> &keys,
                      InsertCallback &callback) const = 0;

  // Generate code to iterate over the entire hash table
  virtual void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                       IterateCallback &callback) const = 0;

  // Generate code to iterate over the entire hash table in vectorized fashion
  virtual void VectorizedIterate(CodeGen &codegen, llvm::Value *ht_ptr,
                                 Vector &selection_vector,
                                 VectorizedIterateCallback &callback) const = 0;

  // Generate code that iterates all the matches
  virtual void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                       const std::vector<codegen::Value> &key,
                       IterateCallback &callback) const = 0;

  // Destroy/cleanup the hash table
  virtual void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const = 0;
};

}  // namespace codegen
}  // namespace peloton