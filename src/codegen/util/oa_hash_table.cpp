//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table.cpp
//
// Identification: src/codegen/util/oa_hash_table.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/oa_hash_table.h"

#include <string.h>

#include "common/logger.h"
#include "common/platform.h"

namespace peloton {
namespace codegen {
namespace util {

// This is an estimation about how many tuples there are in the hash table when
// one is first created. We use, by default, 8K elements. This should be
// extended with result from the query optimizer to approximated number of
// entries in the hash table.
uint32_t OAHashTable::kDefaultInitialSize = 8 * 1024;

// The default capacity of key-value (overflow) lists when we create them
uint32_t OAHashTable::kInitialKVListCapacity = 8;

//===----------------------------------------------------------------------===//
// Initialize the hash table. Here we configure the table to store keys and
// values of the provided size. We also need to find an appropriate size for the
// table given the estimate size.
//===----------------------------------------------------------------------===//
void OAHashTable::Init(uint64_t key_size, uint64_t value_size,
                       uint64_t estimated_num_entries) {
  // Setup the sizes
  key_size_ = key_size;
  value_size_ = value_size;

  // The size of the HashEntry: header + key + value.
  entry_size_ = sizeof(HashEntry) + key_size_ + value_size_;

  // Find a power of two greater than or equal to the estimated size
  num_buckets_ = NextPowerOf2(estimated_num_entries);

  // Bucket mask for mapping the hash value into bucket index into the array
  bucket_mask_ = num_buckets_ - 1;

  // Sanity check
  PL_ASSERT((num_buckets_ & bucket_mask_) == 0);

  // No elements in the table right now
  num_entries_ = num_valid_buckets_ = 0;

  // We maintain a load factor of 50% since this is an easy number to compute
  resize_threshold_ = num_buckets_ >> 1;

  // Create bucket array. We don't use regular "new" since the size of a
  // HashEntry is known at runtime only.
  buckets_ = static_cast<HashEntry *>(malloc(entry_size_ * num_buckets_));

  // Set status code of all buckets to FREE
  InitializeArray(buckets_);
}

//===----------------------------------------------------------------------===//
// Find the next available slot in the key value list. If the list is already
// full then extend the list before storing into it
//
// The argument should be a pointer to the location where the pointer to
// this list is stored, since if we are extending the kv list then
// the original pointer leading us to the list should also be updated
//
// The return value is the starting address for packing value WITHOUT KEY!!!
//
// If resizing of the kv list happens then all pointers on the old
// kv list will be invalidated
//===----------------------------------------------------------------------===//
char *OAHashTable::StoreToKeyValueList(KeyValueList **kv_list_p_p) {
  KeyValueList *kv_list_p = *kv_list_p_p;

  // Size always <= capacity
  PL_ASSERT(kv_list_p->capacity >= kv_list_p->size);

  // We always need this to compute something
  uint32_t size = kv_list_p->size;
  kv_list_p->size++;

  //
  // After this point kv_list_p->size is the size after insertion
  // Use variable "size" instead
  //

  // This is the length including header and current valid data
  // This could also be used when extending the list
  // No matter we extend the list or not, this is the offset
  // of writing into the list
  uint64_t current_length = GetCurrentKeyValueListSize(size);

  // If the kv list is full, we need to extend it
  if (size == kv_list_p->capacity) {
    // Double the capacity
    uint32_t new_capacity = size << 1;

    // Get the new size of the kv list header + 1 key + values
    uint64_t new_kv_list_length = GetCurrentKeyValueListSize(new_capacity);

    kv_list_p = static_cast<KeyValueList *>(malloc(new_kv_list_length));
    PL_ASSERT(kv_list_p != nullptr);

    // Copy from the old memory chunk to the new chunk
    PL_MEMCPY(kv_list_p, *kv_list_p_p, current_length);

    // Update capacity field after copying
    kv_list_p->capacity = new_capacity;

    // Free memory and assign it back to the place where kv_list_p is stored
    free(*kv_list_p_p);
    *kv_list_p_p = kv_list_p;
  }

  return reinterpret_cast<char *>(reinterpret_cast<uint64_t>(kv_list_p) +
                                  current_length);
}

//===----------------------------------------------------------------------===//
// Given a hash value, find the next free slot after the index indicated by
// the hash value. Since we maintain a load factor <= 0.5 so the free slot
// should always be found
//
// The return value is a pointer to the new entry
//===----------------------------------------------------------------------===//
OAHashTable::HashEntry *OAHashTable::FindNextFreeEntry(uint64_t hash_value) {
  uint64_t index = hash_value & bucket_mask_;

  uint64_t current_entry_int =
      reinterpret_cast<uint64_t>(buckets_) + index * entry_size_;

  // Must find one since we maintain load factor <= 50
  while (1) {
    HashEntry *entry_p = reinterpret_cast<HashEntry *>(current_entry_int);

    if (entry_p->IsFree()) {
      return entry_p;
    }

    current_entry_int += entry_size_;
    index++;

    // This implies we should wrap back
    if (index == num_buckets_) {
      index = 0;
      current_entry_int = reinterpret_cast<uint64_t>(buckets_);
    }
  }

  PL_ASSERT(false);
  return nullptr;
}

//===----------------------------------------------------------------------===//
// Set up hash entry OR key value list for the given hash entry pointer.
// This function takes a pointer to HashEntry which __must__ be a valid slot.
//===----------------------------------------------------------------------===//
char *OAHashTable::StoreTuple(HashEntry *entry, uint64_t hash) {
  // If it is free then after resizing we do a probing
  bool entry_is_free = entry->IsFree();

  // Resize!
  //   1. If the target entry is empty prior to a Resize(), then after a
  //      Resize() we can just use the hash value to re-probe the buckets array
  //      to find a new empty slot. We can do this safely because we won't run
  //      into a key collision (though we might have a hash collision, but we
  //      have the hash value).
  //   2. If the target entry is not empty, we need to track the current entry
  //      during resizing (using pointer value), and directly store into that
  //      entry without any probing after resizing. This is because we don't
  //      have the key value available here, and hence, cannot perform key
  //      comparisons in case of key collisions.
  if (NeedsResize()) {
    // This will modify entry if the entry is not free and when it is
    // being moved
    Resize(&entry);

    // If entry is not free then entry points to the entry after resizing
    if (entry_is_free) {
      entry = FindNextFreeEntry(hash);
    }
  }

  // After this point entry is where we store the tuple
  num_entries_++;

  // If the entry is free, we're at the end of the probing chain
  if (entry_is_free) {
    // Mark as a single-value entry
    entry->status = HashEntry::StatusCode::SINGLE_VALUE;

    // StoreValue the hash
    entry->hash = hash;

    // Increase number of elements
    num_valid_buckets_++;

    // Return the region where ***KEY AND VALUE*** should be stored
    return entry->data;
  }

  // The entry is not free. If the entry does not have an overflow value list,
  // we allocate one.
  if (!entry->HasKeyValueList()) {
    // Allocate a chunk that contains kv list header and several value slots
    entry->kv_list = static_cast<KeyValueList *>(malloc(
        GetCurrentKeyValueListSize(OAHashTable::kInitialKVListCapacity)));

    PL_ASSERT(entry->kv_list != nullptr);
    PL_ASSERT(entry->HasKeyValueList());

    // Initialize members - also copy the current data into the kv list
    // to simplify iterating over the hash table
    entry->kv_list->capacity = OAHashTable::kInitialKVListCapacity;
    entry->kv_list->size = 2;

    // Copy the data value ***ONLY*** into the kv list. The key remains inlined
    // in the HashEntry to provide a fast path for key comparison during probing
    // because no matter whether there is a KeyValueList, the key is always at
    // the same location.
    PL_MEMCPY(entry->kv_list->data, entry->data + key_size_, value_size_);

    // Return the second element's payload. The first element has been copied
    // from the HashEntry. This pointer is for dumping value.
    return entry->kv_list->data + value_size_;
  }

  // The entry is not free and already has an overflow list.

  // Find a slot in the kv list and return the address. Since the list might be
  // extended we pass the address of the kv pointer. Also the returned address
  // is for dumping value.
  return StoreToKeyValueList(&entry->kv_list);
}

//===----------------------------------------------------------------------===//
// Initialize all slots in the given entry to FREE state.
//
// This function uses num_buckets_ inside the hash table object, but it still
// requires the caller to pass in an array pointer. This is because in Resize(),
// the size is updated but the newly allocated array hasn't been plugged in yet.
//===----------------------------------------------------------------------===//
void OAHashTable::InitializeArray(HashEntry *entries) {
  uint64_t p = reinterpret_cast<uint64_t>(entries);
  for (uint64_t i = 0; i < num_buckets_; i++) {
    HashEntry *entry = reinterpret_cast<HashEntry *>(p);
    entry->status = HashEntry::StatusCode::FREE;

    // Next entry
    p += entry_size_;
  }
}

//===----------------------------------------------------------------------===//
// Allocate an array that is 2x of the current size, updating the mask (for
// rehash), resize threshold, and element count, and then rearrange all existing
// elements in the old array to their new locations
//
// This function will invalidate all pointers on the old hash table
// So do not call this until all states have been cleared
//===----------------------------------------------------------------------===//
void OAHashTable::Resize(HashEntry **entry_p_p) {
  // Make it an assertion to prevent potential bugs
  PL_ASSERT(NeedsResize());

  LOG_DEBUG("Resizing hash-table from %llu buckets to %llu",
            (unsigned long long)num_buckets_,
            (unsigned long long)num_buckets_ << 1);

  // Double the size of the array
  num_buckets_ <<= 1;

  // Recompute bucket mask (same as in init())
  bucket_mask_ = num_buckets_ - 1;

  // The threshold for resizing also doubles
  resize_threshold_ <<= 1;

  // Allocate the new array
  char *new_buckets = static_cast<char *>(malloc(entry_size_ * num_buckets_));

  // Set it all to status code FREE
  InitializeArray(reinterpret_cast<HashEntry *>(new_buckets));

  // This is the pointer we are tracking
  // If it is not free, then during rehashing we will update its
  // value through the pointer to be the new location in the resized array
  HashEntry *entry_p = *entry_p_p;

  // We use this count to check end condition. This serves as a faster path than
  // iterating over all buckets in the array.
  uint64_t processed_count = 0;

  // This points to the current entry being processed
  char *current_entry_char_p = reinterpret_cast<char *>(buckets_);

  while (processed_count < num_valid_buckets_) {
    // This is meaningless - just to make compiler happy
    HashEntry *current_entry =
        reinterpret_cast<HashEntry *>(current_entry_char_p);

    // Ignore entries that are not occupied by a valid hash key and value
    if (!current_entry->IsFree()) {
      // We have processed one valid entry
      processed_count++;

      // The trick here is that we do not need to recompute the hash since it
      // stays the same as long as the key does not change
      // So just re-mask the hash value and get the index in the new array
      uint64_t new_index = current_entry->hash & bucket_mask_;

      // This is the pointer to the new entry
      char *new_entry_char_p = new_buckets + new_index * entry_size_;

      // Probe the array until we find a free entry
      while (!reinterpret_cast<HashEntry *>(new_entry_char_p)->IsFree()) {
        new_index++;
        new_entry_char_p += entry_size_;

        // If we have reached the end of the array just wrap back
        if (new_index == num_buckets_) {
          new_index = 0;
          new_entry_char_p = new_buckets;
        }
      }

      // If the current entry is not free and is what we are looking for,
      // then return in the argument the new location of that entry
      // the gist behind this is to track a certain key by its entry address
      // since we have guaranteed a given key has only one entry address
      // This should happen only once
      if (entry_p == current_entry) {
        *entry_p_p = reinterpret_cast<HashEntry *>(new_entry_char_p);
      }

      // Copy everything, including is_free flag, hash value and key-value
      // to the new free entry we just found
      PL_MEMCPY(new_entry_char_p, current_entry_char_p, entry_size_);
    }

    // No matter we ignore an entry or not this has to be done
    current_entry_char_p += entry_size_;
  }

  // Free the old array after probing of all elements, and then update
  free(buckets_);
  buckets_ = reinterpret_cast<HashEntry *>(new_buckets);
}

//===----------------------------------------------------------------------===//
// Clean up any resources this hash table has
//
// We need to first scan the array to find out all collision kv lists, delete
// them, and then delete the entire array.
//===----------------------------------------------------------------------===//
void OAHashTable::Destroy() {
  LOG_DEBUG("Cleaning up hash table with %llu entries ...",
            (unsigned long long)num_entries_);

  uint64_t processed_count = 0;
  char *current_entry_char_p = reinterpret_cast<char *>(buckets_);

  // Iterate buckets array looking for overflow kv lists to free
  while (processed_count < num_valid_buckets_) {
    HashEntry *current_entry_p =
        reinterpret_cast<HashEntry *>(current_entry_char_p);

    if (!current_entry_p->IsFree()) {
      // This variable counts non-free variables
      processed_count++;

      // If the entry has a list then free the memory
      if (current_entry_p->HasKeyValueList()) {
        free(current_entry_p->kv_list);
      }
    }

    current_entry_char_p += entry_size_;
  }

  // Free main buckets array
  free(buckets_);
}

OAHashTable::Iterator OAHashTable::begin() { return Iterator(*this, true); }

OAHashTable::Iterator OAHashTable::end() { return Iterator(*this, false); }

//===----------------------------------------------------------------------===//
// ITERATOR STUFF
//===----------------------------------------------------------------------===//

OAHashTable::Iterator::Iterator(OAHashTable &table, bool begin)
    : table_(table) {
  // If we're not creating an Iterator that starts at the beginning, don't
  // initialize anything
  if (!begin) {
    curr_bucket_ = table_.NumBuckets();
    curr_ = nullptr;
    return;
  }

  // Find first entry
  curr_bucket_ = 0;
  curr_ = table_.buckets_;

  NextEntry();
}

OAHashTable::Iterator &OAHashTable::Iterator::operator++() {
  if (kvl_ != nullptr && ++kvl_pos_ < kvl_->size) {
    // We're in KVL, move the position
    return *this;
  }

  curr_bucket_++;
  curr_ = reinterpret_cast<HashEntry *>(reinterpret_cast<uint64_t>(curr_) +
                                        table_.entry_size_);
  NextEntry();

  return *this;
}

bool OAHashTable::Iterator::operator==(const OAHashTable::Iterator &rhs) {
  // TODO: sufficient to check current entry node?
  return curr_bucket_ == rhs.curr_bucket_ && curr_ == rhs.curr_;
}

const char *OAHashTable::Iterator::Key() { return curr_->data; }

const char *OAHashTable::Iterator::Value() {
  if (kvl_ != nullptr) {
    // We're in KVL list, find value at position
    return kvl_->data + (table_.value_size_ * kvl_pos_);
  } else {
    return curr_->data + table_.key_size_;
  }
}

void OAHashTable::Iterator::NextEntry() {
  while (curr_bucket_ < table_.NumBuckets() && curr_->IsFree()) {
    curr_bucket_++;
    curr_ = reinterpret_cast<HashEntry *>(reinterpret_cast<uint64_t>(curr_) +
                                          table_.entry_size_);
  }

  if (curr_bucket_ < table_.NumBuckets()) {
    kvl_ = curr_->HasKeyValueList() ? curr_->kv_list : nullptr;
    kvl_pos_ = 0;
  } else {
    // Exhausted
    curr_ = nullptr;
  }
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton