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

/**
 * The main hash table access class for util::HashTable.
 */
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
   * A callback used when inserting a new entry into the hash table. The caller
   * implements StoreValue() to perform the insertion, and GetValueSize() to
   * indicate the number of bytes needed to store the value associated with the
   * inserted key.
   */
  struct InsertCallback {
    /** Virtual destructor */
    virtual ~InsertCallback() = default;

    /**
     * Serialize the value into a provided memory space in the hash table.
     *
     * @param codegen The codegen instance
     * @param space The memory space for the value
     */
    virtual void StoreValue(CodeGen &codegen, llvm::Value *space) const = 0;

    /**
     * Return the number of bytes for the value
     *
     * @param codegen The codegen instance
     * @return The number of bytes needed to store the value
     */
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

  /**
   * A callback used when iterating over the entries in the hash table.
   * ProcessEntry() is invoked for each entry in the table, or only those
   * entries that match a provided key if a search key is provided.
   */
  struct IterateCallback {
    /** Virtual destructor */
    virtual ~IterateCallback() = default;

    /**
     * The primary callback function for each entry in the table, or for each
     * matching key-value pair when provided a search key.
     *
     * @param codegen The codegen instance
     * @param keys The key stored in the hash table
     * @param values A pointer to a set of bytes where the value is stored
     */
    virtual void ProcessEntry(CodeGen &codegen,
                              const std::vector<codegen::Value> &keys,
                              llvm::Value *values) const = 0;
  };

  class HashTableAccess;

  /**
   * A callback used when performing a batched/vectorized iteration over the
   * entries in the hash table. Iteration may be over the entire table, or a
   * subset of the table if a matching probing key was provided.
   */
  struct VectorizedIterateCallback {
    /** Virtual destructor */
    virtual ~VectorizedIterateCallback() = default;

    /**
     * Process a vector of entries in the hash table.
     *
     * @param codegen The codegen instance
     * @param start
     * @param end
     * @param selection_vector A vector containing indexes of valid entries
     * @param access A hash-table random-access helper
     */
    virtual void ProcessEntries(CodeGen &codegen, llvm::Value *start,
                                llvm::Value *end, Vector &selection_vector,
                                HashTableAccess &access) const = 0;
  };

  /**
   * Convenience class proving a random access interface over the hash-table
   */
  class HashTableAccess {
   public:
    /** Virtual destructor */
    virtual ~HashTableAccess() = default;

    /**
     * Extracts the key of an entry at a given index into the hash table storing
     * results into the output 'keys' vector.
     *
     * @param codegen The codegen instance
     * @param index The index in the directory
     * @param[out] keys Where each column of the key is stored
     */
    // Extract the keys for the bucket at the given index
    virtual void ExtractBucketKeys(CodeGen &codegen, llvm::Value *index,
                                   std::vector<codegen::Value> &keys) const = 0;

    /**
     * Returns a pointer to a value stored at the entry at the given index.
     *
     * @param codegen The codegen instance
     * @param index An index in the directory
     * @return A pointer to where the value is serialized
     */
    virtual llvm::Value *BucketValue(CodeGen &codegen,
                                     llvm::Value *index) const = 0;
  };

  /**
   * A struct storing the result of a probe into the hash table
   */
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

  virtual void Init(CodeGen &codegen, llvm::Value *ht_ptr) const;
  virtual void Init(CodeGen &codegen, llvm::Value *exec_ctx,
                    llvm::Value *ht_ptr) const;

  virtual void ProbeOrInsert(CodeGen &codegen, llvm::Value *ht_ptr,
                             llvm::Value *hash,
                             const std::vector<codegen::Value> &key,
                             ProbeCallback &probe_callback,
                             InsertCallback &insert_callback) const;

  virtual ProbeResult ProbeOrInsert(
      CodeGen &codegen, llvm::Value *ht_ptr, llvm::Value *hash,
      const std::vector<codegen::Value> &key) const;

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

  virtual void Iterate(CodeGen &codegen, llvm::Value *ht_ptr,
                       IterateCallback &callback) const;

  virtual void VectorizedIterate(CodeGen &codegen, llvm::Value *ht_ptr,
                                 Vector &selection_vector,
                                 VectorizedIterateCallback &callback) const;

  virtual void FindAll(CodeGen &codegen, llvm::Value *ht_ptr,
                       const std::vector<codegen::Value> &key,
                       IterateCallback &callback) const;

  virtual void Destroy(CodeGen &codegen, llvm::Value *ht_ptr) const;

 private:
  uint32_t value_size_;

  // The storage strategy we use to store the lookup keys inside every HashEntry
  CompactStorage key_storage_;
};

}  // namespace codegen
}  // namespace peloton