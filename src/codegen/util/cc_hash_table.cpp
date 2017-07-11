//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cc_hash_table.cpp
//
// Identification: src/codegen/util/cc_hash_table.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/util/cc_hash_table.h"

#include "common/logger.h"
#include "common/platform.h"

namespace peloton {
namespace codegen {
namespace util {

// TODO: worry about growing the table

//===----------------------------------------------------------------------===//
// Constructor
//===----------------------------------------------------------------------===//
CCHashTable::CCHashTable(uint64_t size) : num_elements_(0) {
  // Find the number of buckets so that the expected hash chain
  // size is < kMaxHashChainSize
  uint64_t guess = size / kMaxHashChainSize;
  uint64_t required_buckets_in_bits = 64 - CountLeadingZeroes(guess) - 1;
  num_buckets_ = 1 << required_buckets_in_bits;
  bucket_mask_ = num_buckets_ - 1;
  buckets_ =
      static_cast<HashEntry**>(malloc(sizeof(HashEntry*) * num_buckets_));
  for (uint64_t i = 0; i < num_buckets_; i++) {
    buckets_[i] = nullptr;
  }
}

//===----------------------------------------------------------------------===//
// Destructor
//===----------------------------------------------------------------------===//
CCHashTable::~CCHashTable() { Destroy(); }

//===----------------------------------------------------------------------===//
// Initialize the hash table
//===----------------------------------------------------------------------===//
void CCHashTable::Init() {
  // Find the number of buckets so that the expected hash chain
  // size is < kMaxHashChainSize
  uint64_t size = 1 * 1024 * 1024 * 256;
  uint64_t guess = size / kMaxHashChainSize;
  uint64_t required_buckets_in_bits = 64 - CountLeadingZeroes(guess) - 1;
  num_buckets_ = 1 << required_buckets_in_bits;
  bucket_mask_ = num_buckets_ - 1;
  buckets_ =
      static_cast<HashEntry**>(malloc(sizeof(HashEntry*) * num_buckets_));
  for (uint64_t i = 0; i < num_buckets_; i++) {
    buckets_[i] = nullptr;
  }
}

//===----------------------------------------------------------------------===//
// Make room for a key-value entry whose hash value and total size are equal to
// the values provided as parameters to the method
//===----------------------------------------------------------------------===//
char* CCHashTable::StoreTuple(uint64_t hash, uint32_t size) {
  uint64_t bucket_num = hash % num_buckets_;
  HashEntry* entry = static_cast<HashEntry*>(malloc(sizeof(HashEntry) + size));
  entry->hash = hash;
  entry->next = buckets_[bucket_num];
  buckets_[bucket_num] = entry;
  num_elements_++;
  return entry->data;
}

//===----------------------------------------------------------------------===//
// Clean up any resources this hash table has
//===----------------------------------------------------------------------===//
void CCHashTable::Destroy() {
  LOG_DEBUG("Cleaning up hash table with %llu entries ...", (unsigned long long) num_elements_);
  for (uint64_t i = 0; i < num_buckets_; i++) {
    uint32_t chain_length = 0;
    HashEntry* e = buckets_[i];
    while (e != nullptr) {
      HashEntry* next = e->next;
      free(e);
      e = next;
      chain_length++;
    }
    if (chain_length > kMaxHashChainSize) {
      LOG_DEBUG("Bucket %llu chain length = %u ...", (unsigned long long) i, chain_length);
    }
  }
  free(buckets_);
}

//===----------------------------------------------------------------------===//
// Return an iterator that points to the start of the hash table
//===----------------------------------------------------------------------===//
CCHashTable::iterator CCHashTable::begin() { return iterator(*this, true); }

//===----------------------------------------------------------------------===//
// Return an iterator that points to the end of the hash table
//===----------------------------------------------------------------------===//
CCHashTable::iterator CCHashTable::end() { return iterator(*this, false); }

//===----------------------------------------------------------------------===//
// HashTable's iterator constructor
//===----------------------------------------------------------------------===//
CCHashTable::iterator::iterator(CCHashTable& table, bool begin)
    : table_(table) {
  // If we're not creating an iterator that starts at the beginning, don't
  // initialize anything
  if (!begin) {
    curr_bucket_ = 0;
    curr_ = nullptr;
    return;
  }

  // Find first entry
  curr_bucket_ = 0;
  curr_ = table_.buckets_[curr_bucket_];
  while (curr_bucket_ < table_.NumBuckets()) {
    if (table_.buckets_[curr_bucket_] != nullptr) {
      curr_ = table_.buckets_[curr_bucket_];
      break;
    }
    curr_bucket_++;
  }
}

//===----------------------------------------------------------------------===//
// Find the next HashEntry that is valid
//===----------------------------------------------------------------------===//
CCHashTable::iterator& CCHashTable::iterator::operator++() {
  if (curr_ != nullptr && curr_->next != nullptr) {
    curr_ = curr_->next;
    return *this;
  }
  while (curr_bucket_ < table_.NumBuckets()) {
    if (table_.buckets_[curr_bucket_] != nullptr) {
      curr_ = table_.buckets_[curr_bucket_];
      break;
    }
    curr_bucket_++;
  }
  return *this;
}

//===----------------------------------------------------------------------===//
// Check if two iterators are equal. In this case, we just check whether the two
// point to the same bucket and point to the same actual hash entry node
//===----------------------------------------------------------------------===//
bool CCHashTable::iterator::operator==(const CCHashTable::iterator& rhs) {
  // TODO: sufficient to check current entry node?
  return curr_bucket_ == rhs.curr_bucket_ && curr_ == rhs.curr_;
}

//===----------------------------------------------------------------------===//
// Check if two iterators are not equal
//===----------------------------------------------------------------------===//
bool CCHashTable::iterator::operator!=(const CCHashTable::iterator& rhs) {
  return !(*this == rhs);
}

//===----------------------------------------------------------------------===//
// Dereference the iterator, returning the data storage segment
//===----------------------------------------------------------------------===//
const char* CCHashTable::iterator::operator*() { return curr_->data; }

}  // namespace util
}  // namespace codegen
}  // namespace peloton