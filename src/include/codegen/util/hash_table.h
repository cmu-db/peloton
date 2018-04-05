//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.h
//
// Identification: src/include/codegen/util/hash_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "executor/executor_context.h"

namespace peloton {

namespace type {
class AbstractPool;
}  // namespace type

namespace codegen {
namespace util {

class HashTable {
 public:
  /// Constructor
  HashTable(::peloton::type::AbstractPool &memory, uint64_t key_size,
            uint64_t value_size);

  /// Destructor
  ~HashTable();

  /**
   * Initialize the provided hash table
   *
   * @param table The table we're setting up
   * @param key_size The size of the keys in bytes
   * @param value_size The size of the values in bytes
   */
  static void Init(HashTable &table, executor::ExecutorContext &exec_ctx,
                   uint64_t key_size, uint64_t value_size);

  /**
   * Clean up all resources allocated by the provided table
   *
   * @param table The table we're cleaning up
   */
  static void Destroy(HashTable &table);

  /**
   * Make room in the hash-table to store a new key-value pair with the provided
   * hash value
   *
   * @param hash The hash value of the tuple that will be inserted
   * @return A memory region where the key and value can be stored
   */
  char *StoreTupleLazy(uint64_t hash);

  /**
   * Make room in the hash-table to store the new key-value pair.
   *
   * @param hash
   * @return
   */
  char *StoreTuple(uint64_t hash);

  /**
   *
   */
  void BuildLazy();

  /**
   *
   * @param thread_states
   * @param hash_table_offset
   */
  void ReserveLazy(const executor::ExecutorContext::ThreadStates &thread_states,
                   uint32_t hash_table_offset);

  /**
   *
   * @param other
   */
  void MergeLazyUnfinished(const HashTable &other);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  uint64_t NumElements() const { return num_elems_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Testing Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Insert a key-value pair into the hash-table. This function is used mostly
   * for testing since we specialize insertions in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void Insert(uint64_t hash, const Key &key, const Value &value);

  /**
   * Probe a key in the hash table. This function is used mostly for testing.
   * Probing is completely specialized in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to probe in the table
   * @param[out] value The value associated with the key in the table
   * @return True if a value was found. False otherwise.
   */
  template <typename Key, typename Value>
  bool Probe(uint64_t hash, const Key &key, Value &value);

  // An entry in the hash table
  struct Entry {
    uint64_t hash;
    Entry *next;
    char data[0];
  };

 private:
  Entry *AcquireEntrySlot();

  // Does the hash table need resizing?
  bool NeedsResize() const { return num_elems_ == capacity_; }

  // Resize the hash table
  void Resize();

 private:
  // A chunk of memory that stores tuple data
  struct MemoryBlock {
    MemoryBlock *next;
    char data[0];
  };

 private:
  // The memory allocator used for all allocations in this hash table
  ::peloton::type::AbstractPool &memory_;

  // The size of an entry in the hash table. Includes space for entry metadata,
  // key, and value
  uint64_t entry_size_;

  // The directory of the hash table
  Entry **directory_;
  uint64_t directory_size_;
  uint64_t directory_mask_;

  // A linked list of memory blocks where tuples are stored
  MemoryBlock *block_;
  char *next_tuple_pos_;
  uint64_t available_bytes_;

  // The number of elements stored in this hash table, and the max before it
  // needs to be resized
  uint64_t num_elems_;
  uint64_t capacity_;

  // Info about partitions
  // ...
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value>
void HashTable::Insert(uint64_t hash, const Key &key, const Value &value) {
  auto *data = StoreTupleLazy(hash);
  *reinterpret_cast<Key *>(data) = key;
  *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
}

template <typename Key, typename Value>
bool HashTable::Probe(uint64_t hash, const Key &key, Value &value) {
  // Initial index in the directory
  uint64_t index = hash & directory_mask_;

  auto *entry = directory_[index];
  if (entry == nullptr) {
    return false;
  }
  while (entry != nullptr) {
    if (entry->hash == hash && *reinterpret_cast<Key *>(entry->data) == key) {
      value = *reinterpret_cast<Value *>(entry->data + sizeof(Key));
      return true;
    }
    entry = entry->next;
  }

  // Not found
  return false;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton