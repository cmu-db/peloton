//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_table.cpp
//
// Identification: src/codegen/util/hash_table.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/hash_table.h"

#include "common/platform.h"
#include "type/abstract_pool.h"

namespace peloton {
namespace codegen {
namespace util {

static const uint32_t kDefaultNumElements = 256;
static const uint32_t kNumBlockElems = 1024;
static const uint32_t kDefaultEstimatePrecision = 5;

static_assert((kDefaultNumElements & (kDefaultNumElements - 1)) == 0,
              "Default number of elements must be a power of two");

////////////////////////////////////////////////////////////////////////////////
///
/// EntryBuffer
///
////////////////////////////////////////////////////////////////////////////////

HashTable::EntryBuffer::EntryBuffer(::peloton::type::AbstractPool &memory,
                                    uint32_t entry_size)
    : memory_(memory), entry_size_(entry_size) {
  // We also need to allocate some space to store tuples. Tuples are stored
  // externally from the main hash table in a separate values memory space.
  uint64_t block_size = sizeof(MemoryBlock) + (entry_size_ * kNumBlockElems);
  block_ = reinterpret_cast<MemoryBlock *>(memory_.Allocate(block_size));
  block_->next = nullptr;

  // Set the next tuple write position and the available bytes
  next_entry_ = block_->data;
  available_bytes_ = block_size - sizeof(MemoryBlock);
}

HashTable::EntryBuffer::~EntryBuffer() {
  // Free all the blocks we've allocated
  MemoryBlock *block = block_;
  while (block != nullptr) {
    MemoryBlock *next = block->next;
    memory_.Free(block);
    block = next;
  }
  block_ = nullptr;
}

HashTable::Entry *HashTable::EntryBuffer::NextFree() {
  if (entry_size_ > available_bytes_) {
    uint64_t block_size = sizeof(MemoryBlock) + (entry_size_ * kNumBlockElems);
    auto *new_block =
        reinterpret_cast<MemoryBlock *>(memory_.Allocate(block_size));
    new_block->next = block_;
    block_ = new_block;
    next_entry_ = new_block->data;
    available_bytes_ = block_size - sizeof(MemoryBlock);
  }

  auto *entry = reinterpret_cast<Entry *>(next_entry_);
  entry->next = nullptr;

  next_entry_ += entry_size_;
  available_bytes_ -= entry_size_;

  return entry;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Hash Table
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This hash-table uses an open-addressing probing scheme
 *
 */

HashTable::HashTable(::peloton::type::AbstractPool &memory, uint32_t key_size,
                     uint32_t value_size)
    : memory_(memory),
      directory_(nullptr),
      directory_size_(0),
      directory_mask_(0),
      entry_buffer_(memory, Entry::Size(key_size, value_size)),
      num_elems_(0),
      capacity_(kDefaultNumElements),
      unique_key_estimate_(libcount::HLL::Create(kDefaultEstimatePrecision)) {
  // Upon creation, we allocate room for kDefaultNumElements in the hash table.
  // We assume 50% load factor on the directory, thus the directory size is
  // twice the number of elements.
  directory_size_ = capacity_ * 2;
  directory_mask_ = directory_size_ - 1;
  directory_ = static_cast<Entry **>(
      memory_.Allocate(sizeof(Entry *) * directory_size_));
  PELOTON_MEMSET(directory_, 0, directory_size_);
}

HashTable::~HashTable() {
  // Free the directory
  if (directory_ != nullptr) {
    memory_.Free(directory_);
    directory_ = nullptr;
  }
}

void HashTable::Init(HashTable &table, executor::ExecutorContext &exec_ctx,
                     uint32_t key_size, uint32_t value_size) {
  new (&table) HashTable(*exec_ctx.GetPool(), key_size, value_size);
}

void HashTable::Destroy(HashTable &table) { table.~HashTable(); }

char *HashTable::InsertLazy(uint64_t hash) {
  // Since this is a lazy insertion, we just need to acquire/allocate an entry
  // from storage. It is assumed that actual construction of the hash table is
  // done by a subsequent call to BuildLazy() only after ALL lazy insertions
  // have completed.
  auto *entry = entry_buffer_.NextFree();
  entry->hash = hash;

  // Insert the entry into the linked list in the first directory slot
  if (directory_[0] == nullptr) {
    // This is the first entry
    directory_[0] = directory_[1] = entry;
  } else {
    PELOTON_ASSERT(directory_[1] != nullptr);
    directory_[1]->next = entry;
    directory_[1] = entry;
  }

  num_elems_++;

  // Return data pointer for key/value storage
  return entry->data;
}

char *HashTable::Insert(uint64_t hash) {
  // Resize the hash table if needed
  if (NeedsResize()) {
    Resize();
  }

  // Acquire/allocate an entry from storage
  Entry *entry = entry_buffer_.NextFree();
  entry->hash = hash;

  // Insert into hash table
  uint64_t index = hash & directory_mask_;
  entry->next = directory_[index];
  directory_[index] = entry;

  num_elems_++;

  // Return data pointer for key/value storage
  return entry->data;
}

void HashTable::BuildLazy() {
  // Grab entry head
  Entry *head = directory_[0];

  // Clean up old directory
  memory_.Free(directory_);

  // At this point, all the lazy insertions are assumed to have completed. We
  // can allocate a perfectly sized hash table with 50% load factor.
  //
  // TODO: Use sketches to estimate the real # of unique elements
  // TODO: Perhaps change probing strategy based on estimate?

  directory_size_ = NextPowerOf2(num_elems_) * 2;
  directory_mask_ = directory_size_ - 1;
  directory_ = static_cast<Entry **>(
      memory_.Allocate(sizeof(Entry *) * directory_size_));
  PELOTON_MEMSET(directory_, 0, directory_size_);

  // Now insert all elements into the directory
  while (head != nullptr) {
    // Compute the target index
    // Stash the next linked-list entry into a temporary variable
    // Connect the current entry into the bucket chain
    // Move along
    uint64_t index = head->hash & directory_mask_;
    Entry *next = head->next;
    head->next = directory_[index];
    directory_[index] = head;
    head = next;
  }
}

void HashTable::ReserveLazy(
    const executor::ExecutorContext::ThreadStates &thread_states,
    uint32_t hash_table_offset) {
  // Determine the total number of tuples stored across each hash table
  uint64_t total_size = 0;
  for (uint32_t i = 0; i < thread_states.NumThreads(); i++) {
    auto *hash_table = reinterpret_cast<HashTable *>(
        thread_states.AccessThreadState(i) + hash_table_offset);
    total_size += hash_table->NumElements();
  }

  // TODO: Combine sketches to estimate the true unique # of elements

  // Perfectly size the hash table
  num_elems_ = 0;
  capacity_ = NextPowerOf2(total_size);

  directory_size_ = capacity_ * 2;
  directory_mask_ = directory_size_ - 1;
  directory_ = static_cast<Entry **>(
      memory_.Allocate(sizeof(Entry *) * directory_size_));
}

void HashTable::MergeLazyUnfinished(const HashTable &other) {
  // Begin with the head of the linked list of entries, stored in the first
  // directory entry
  PELOTON_ASSERT(other.directory_[0] != nullptr);
  auto *head = other.directory_[0];

  while (head != nullptr) {
    // Compute the index and stash the next entry in the linked list
    uint64_t index = head->hash & directory_mask_;
    Entry *next = head->next;

    // Try to CAS in this entry into the directory
    Entry *curr;
    do {
      curr = directory_[index];
      head->next = curr;
    } while (!atomic_cas(directory_ + index, curr, head));

    // Success, move along
    head = next;
  }
}

void HashTable::Resize() {
  // Sanity check
  PELOTON_ASSERT(NeedsResize());

  // Double the capacity
  capacity_ *= 2;

  // Allocate the new directory with 50% fill factor
  uint64_t new_dir_size = capacity_ * 2;
  uint64_t new_dir_mask = new_dir_size - 1;
  auto *new_dir =
      static_cast<Entry **>(memory_.Allocate(sizeof(Entry *) * new_dir_size));
  PELOTON_MEMSET(new_dir, 0, new_dir_size);

  // Insert all old directory entries into new directory
  for (uint32_t i = 0; i < directory_size_; i++) {
    auto *entry = directory_[i];
    if (entry == nullptr) {
      continue;
    }
    // Traverse bucket chain, reinserting into new table
    while (entry != nullptr) {
      uint64_t index = entry->hash & new_dir_mask;
      Entry *next = entry->next;
      entry->next = directory_[index];
      directory_[index] = entry;
      entry = next;
    }
  }

  // Done. First free the old directory.
  memory_.Free(directory_);

  // Set up the new directory
  directory_size_ = new_dir_size;
  directory_mask_ = new_dir_mask;
  directory_ = new_dir;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton
