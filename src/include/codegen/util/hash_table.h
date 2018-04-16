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
   * Make room in the hash table's storage space to store a new key-value pair
   * with the provided hash value, but defer insertion into the directory to a
   * later pointer.
   *
   * This is mainly used for two-phase hash-joins, where all insertions into
   * the table happen in one phase, and all probes into the hash table happen
   * in a separate phase. In such cases, BuildLazy() must be called to properly
   * construct the hash table.
   *
   * @param hash The hash value of the tuple that will be inserted
   * @return A memory region where the key and value can be stored
   */
  char *InsertLazy(uint64_t hash);

  /**
   * Make room in the hash table to store the new key-value pair.
   *
   * @param hash
   * @return
   */
  char *Insert(uint64_t hash);

  /**
   * This function builds a hash table over all elements that have been lazily
   * inserted into the hash table. It is assumed that this hash table is being
   * used in two-phase mode, i.e., where all insertions are performed first,
   * followed by a series of probes.
   *
   * After this call, the hash table will not accept any more insertions. The
   * hash table will be frozen.
   */
  void BuildLazy();

  /**
   * This function inspects the size of each thread-local hash table stored in
   * the thread states argument to perfectly size a 50% loaded hash table.
   *
   * This function is called during parallel hash table builds once each thread
   * has finished constructing its own thread-local hash table. The final phase
   * is to build a global hash table (this one) with pointers into thread-local
   * hash tables.
   *
   * @param thread_states Where thread-local hash tables are located
   * @param hash_table_offset The offset into each state where the thread-local
   * hash table can be found.
   */
  void ReserveLazy(const executor::ExecutorContext::ThreadStates &thread_states,
                   uint32_t hash_table_offset);

  /**
   * This function is called to "merge" the contents of the provided hash table
   * into this table. Each hash table is assumed to have been built lazily
   * constructed and unfinished; that is, each able has not constructed a
   * directory hash table, but has buffered a series of tuples into hash table
   * memory.
   *
   * This function is called from different threads!
   *
   * @param The hash table whose contents we will merge into this one..
   */
  void MergeLazyUnfinished(HashTable &other);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  uint64_t NumElements() const { return num_elems_; }
  uint64_t Capacity() const { return capacity_; }
  double LoadFactor() const { return num_elems_ / 1.0 / directory_size_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Testing Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * Insert a key-value pair lazily into the hash table. This function is used
   * mostly for testing since we specialize insertions in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void TypedInsertLazy(uint64_t hash, const Key &key, const Value &value);

  /**
   * Insert a key-value pair into the hash table. This function is used mostly
   * for testing since we specialize insertions in generated code.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void TypedInsert(uint64_t hash, const Key &key, const Value &value);

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
  bool TypedProbe(uint64_t hash, const Key &key,
                  std::function<void(const Value &)> &consumer);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Helper Classes
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * An entry in the hash table that stores a key and value. Entries are
   * variable-sized structs. In memory, the key and value bytes are stored
   * contiguously after the next pointer. The layout is thus:
   *
   * +--------------+---------------+-------------------+--------------------+
   * |  hash value  | next pointer  | sizeof(Key) bytes | sizeof(Val) bytes) |
   * +--------------+---------------+-------------------+--------------------+
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
    /**
     * Constructor for a buffer of entries.
     *
     * @param memory The memory pool to source memory for entries from
     * @param entry_size The size of an entry
     */
    EntryBuffer(::peloton::type::AbstractPool &memory, uint32_t entry_size);

    /**
     * Destructor.
     */
    ~EntryBuffer();

    /**
     * Return a pointer to the next available Entry slot. If insufficient memory
     * is available, memory is taken from the pool.
     *
     * @return A pointer to a free Entry slot.
     */
    Entry *NextFree();

    /**
     * Transfer all allocated memory blocks in this entry buffer into the target
     * buffer.
     *
     * @param target Buffer where all blocks are transferred to
     */
    void TransferMemoryBlocks(EntryBuffer &target);

   private:
    // This struct represents a chunk of heap memory. We chain together these
    // chunks to avoid the need for a std::vector.
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
void HashTable::TypedInsert(uint64_t hash, const Key &key, const Value &value) {
  auto *data = Insert(hash);
  *reinterpret_cast<Key *>(data) = key;
  *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
}

template <typename Key, typename Value>
bool HashTable::TypedProbe(uint64_t hash, const Key &key,
                           std::function<void(const Value &)> &consumer) {
  // Initial index in the directory
  uint64_t index = hash & directory_mask_;

  auto *entry = directory_[index];
  if (entry == nullptr) {
    return false;
  }

  bool found = false;
  while (entry != nullptr) {
    if (entry->hash == hash && *reinterpret_cast<Key *>(entry->data) == key) {
      auto *value = reinterpret_cast<Value *>(entry->data + sizeof(Key));
      consumer(*value);
      found = true;
    }
    entry = entry->next;
  }

  // Not found
  return found;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton