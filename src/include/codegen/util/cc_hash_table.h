//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table.h
//
// Identification: src/include/codegen/util/cc_hash_table.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

namespace peloton {
namespace codegen {
namespace util {

//===----------------------------------------------------------------------===//
// This is a closed-addressing hash-table data structure that employs a dynamic
// collision chain to resolve hash collisions. This hash table is tailored for
// aggregations and hash joins in Peloton. It is structured as a contiguous
// array of buckets, where each bucket points to a list of HashEntry nodes
// linked together into a list.
//
// A HashEntry stores the hash value of the node, a pointer to the next
// hash-entry and the data. The data is stored contiguously at the end of the
// HashEntry.
//===----------------------------------------------------------------------===//
class CCHashTable {
 public:
  // On average, we want to ensure that the length of any hash chain is less
  // than 128
  static const uint16_t kMaxHashChainSize = 128;

  // A HashEntry
  struct HashEntry {
    uint64_t hash;    // The hash value
    HashEntry* next;  // The next entry in the chain
    char data[0];     // The contiguous data storage segment
  };

  // Constructor. Create a new hashtable initially capable of storing up to
  // 'size' elements
  CCHashTable(uint64_t size);
  // Destructor
  ~CCHashTable();

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  // The number of buckets
  uint64_t NumBuckets() const { return num_buckets_; }
  // The total number of elements in this hash table
  uint64_t NumElements() const { return num_elements_; }

  //===--------------------------------------------------------------------===//
  // MODIFIERS
  //===--------------------------------------------------------------------===//

  // Perform some initialization
  void Init();

  // Make room in the hash table to store a new key-value pair whose hash and
  // total size are equal to those provided as parameters to the function call
  char *StoreTuple(uint64_t hash, uint32_t size);

  // Clean up any resources this hash table has
  void Destroy();

  //===--------------------------------------------------------------------===//
  // Iteration
  //===--------------------------------------------------------------------===//
  class iterator {
    friend class CCHashTable;

   public:
    // Move the iterator forward
    iterator& operator++();
    // (In)Equality check
    bool operator==(const iterator& rhs);
    bool operator!=(const iterator& rhs);
    // Dereference
    const char* operator*();

   private:
    // Private constructor, so only the HashTable can create an iterator. Begin
    // tells us whether we've finished or not
    iterator(CCHashTable& table, bool begin);
    // The hash table itself
    CCHashTable& table_;
    // The current bucket we're iterating over
    uint64_t curr_bucket_;
    // The next entry to offer
    HashEntry* curr_;
  };

  // Methods to begin iteration and find the end of the iterator
  iterator begin();
  iterator end();

 private:
  // XXX: Remember, if you alter any of the field below, you'll need to modify
  //      HashTableProxy. Hopefully, you'll get a compile-time error about this.

  // The buckets
  HashEntry **buckets_;
  // The total number of buckets
  uint64_t num_buckets_;
  // The mask we use to find the bucket a given hash falls into
  uint64_t bucket_mask_;
  // Total number of entries in the hash table
  uint64_t num_elements_;
};

}  // namespace util
}  // namespace codegen
}  // namespace peloton