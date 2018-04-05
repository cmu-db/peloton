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

#include "libcount/hll.h"

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
  HashTable(::peloton::type::AbstractPool &memory, uint32_t key_size,
            uint32_t value_size);

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
                   uint32_t key_size, uint32_t value_size);

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
  char *InsertLazy(uint64_t hash);

  /**
   * Make room in the hash-table to store the new key-value pair.
   *
   * @param hash
   * @return
   */
  char *Insert(uint64_t hash);

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
   * @param
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
  void TypedInsertLazy(uint64_t hash, const Key &key, const Value &value);

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
  bool TypedProbe(uint64_t hash, const Key &key, Value &value);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Helper Classes
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * An entry in the hash table that stores a key and value
   */
  struct Entry {
    uint64_t hash;
    Entry *next;
    char data[0];

    static uint32_t Size(uint32_t key_size, uint32_t value_size) {
      return sizeof(Entry) + key_size + value_size;
    }
  };

  /**
   * An entry allocator
   */
  class EntryBuffer {
   public:
    EntryBuffer(::peloton::type::AbstractPool &memory, uint32_t entry_size);

    ~EntryBuffer();

    Entry *NextFree();

   private:
    struct MemoryBlock {
      MemoryBlock *next;
      char data[0];
    };

    // The memory pool where block allocations are sourced
    ::peloton::type::AbstractPool &memory_;
    // The sizes of each entry
    uint32_t entry_size_;
    // The current active block
    MemoryBlock *block_;
    // A pointer into the block where the next position is
    char *next_entry_;
    // The number of available bytes left in the block
    uint64_t available_bytes_;
  };

 private:
  // Does the hash table need resizing?
  bool NeedsResize() const { return num_elems_ == capacity_; }

  // Resize the hash table
  void Resize();

 private:
  // The memory allocator used for all allocations in this hash table
  ::peloton::type::AbstractPool &memory_;

  // The directory of the hash table
  Entry **directory_;
  uint64_t directory_size_;
  uint64_t directory_mask_;

  // Entry allocator
  EntryBuffer entry_buffer_;

  // The number of elements stored in this hash table, and the max before it
  // needs to be resized
  uint64_t num_elems_;
  uint64_t capacity_;

  // Info about partitions
  // ...

  // Stats
  std::unique_ptr<libcount::HLL> unique_key_estimate_;
};

////////////////////////////////////////////////////////////////////////////////
///
/// Implementation below
///
////////////////////////////////////////////////////////////////////////////////

template <typename Key, typename Value>
void HashTable::TypedInsertLazy(uint64_t hash, const Key &key,
                                const Value &value) {
  auto *data = InsertLazy(hash);
  *reinterpret_cast<Key *>(data) = key;
  *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
}

template <typename Key, typename Value>
bool HashTable::TypedProbe(uint64_t hash, const Key &key, Value &value) {
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