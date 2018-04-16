//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// oa_hash_table.h
//
// Identification: src/include/codegen/util/oa_hash_table.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>

namespace peloton {
namespace codegen {
namespace util {

//===----------------------------------------------------------------------===//
// This is the primary hash-table data structure used for aggregations and hash
// joins in Peloton. It is an open-addressing hash-table that uses linear
// probing to resolve collisions.
//
// Buckets are structured as a contiguous array of HashEntry structs.
//
// A HashEntry struct stores a status code, the hash value of the entry and the
// data itself. If there are multiple entries for a key, then the first
// key-value pair is stored inside the HashEntry itself to make common case
// fast; all other values are stored sequentially in an external KeyValueList
// structure, also in the form of key-value pair.
//===----------------------------------------------------------------------===//
class OAHashTable {
 public:
  static uint32_t kDefaultInitialSize;
  static uint32_t kInitialKVListCapacity;

  // The structure used for holding multiple values having identical keys.
  // We maintain and grow this data structure in a manner similar to std::vector
  struct KeyValueList {
    // Number of key-value slots allocated inside this chunk. This doubles every
    // time we grow the list.
    uint32_t capacity;

    // Number of valid elements inside this chunk.
    // size <= capacity
    uint32_t size;

    // Extra space for storing values, up to # capacity.
    char data[0];
  };

  // A HashEntry representing a bucket in the bucket array.
  struct HashEntry {
    enum class StatusCode : uint64_t {
      FREE = 0,
      SINGLE_VALUE = 1,
    };

    union {
      // This is the head of overflow key value list.
      // Used when the key is associated with multiple values.
      KeyValueList *kv_list;
      StatusCode status;
    };

    // The hash value. We store hash values because the hash-table
    uint64_t hash;

    // The contiguous data storage segment. We store key first, and then value.
    char data[0];

    // Whether the entry is free
    bool IsFree() const { return status == StatusCode::FREE; }

    // Whether the entry has extra key-value pair stored in the
    // key value list. For all an undefined enum value, we assume it's a pointer
    bool HasKeyValueList() const { return status > StatusCode::SINGLE_VALUE; }
  };

  /// Constructor
  OAHashTable(uint64_t key_size, uint64_t value_size,
              uint64_t estimated_num_entries = kDefaultInitialSize);

  /// Destructor
  ~OAHashTable();

  /**
   * Initialize the provided hash table
   *
   * @param table The table we're setting up
   * @param key_size The size of the keys in bytes
   * @param value_size The size of the values in bytes
   * @param estimated_num_entries An initial estimate of the number of entries
   * that will be stored in the table
   */
  static void Init(OAHashTable &table, uint64_t key_size, uint64_t value_size,
                   uint64_t estimated_num_entries);

  /**
   * Clean up all resources allocated by the provided table
   *
   * @param table The table we're cleaning up
   */
  static void Destroy(OAHashTable &table);

  /**
   * Insert a key-value pair into the hash-table. Mostly used for testing.
   *
   * @param hash The hash value of the key
   * @param key The key to store in the table
   * @param value The value to store in the value
   */
  template <typename Key, typename Value>
  void Insert(uint64_t hash, const Key &key, const Value &value);

  /**
   * Probe a key in the hash table. Doesn't deal with duplicate values. Mostly
   * for testing.
   *
   * @param hash
   * @param key
   * @param value
   * @return
   */
  template <typename Key, typename Value>
  bool Probe(uint64_t hash, const Key &key,
             std::function<void(const Value &)> &consumer);

  /**
   * Make room in the hash-table to store a new key-value pair. The provided
   * hash table entry is the initial location where the caller would like to
   * insert.
   *
   * Pre-condition: the given hash-entry is either free or has pre-existing data
   * with the same key as that which is to be inserted.
   *
   * @param entry The initial lookup location in the table to store the tuple
   * @param hash The hash value of the tuple
   *
   * @return A memory region where the key and value can be stored
   */
  char *StoreTuple(HashEntry *entry, uint64_t hash);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// The number of buckets
  uint64_t NumBuckets() const { return num_buckets_; }

  /// The total number of elements in this hash-table
  uint64_t NumEntries() const { return num_entries_; }

  /// The total number of valid buckets
  uint64_t NumOccupiedBuckets() const { return num_valid_buckets_; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Iterator
  ///
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @brief Our hash table iterator
   */
  class Iterator {
    friend class OAHashTable;

   public:
    // Move the iterator forward
    Iterator &operator++();
    // (In)Equality check
    bool operator==(const Iterator &rhs);
    bool operator!=(const Iterator &rhs) { return !(*this == rhs); }

    // Dereference
    const char *Key();
    const char *Value();

   private:
    // Constructor. Begin tells us whether we've finished or not
    Iterator(OAHashTable &table, bool begin);

    // Find the next occupied entry
    void NextEntry();

   private:
    // The hash-table itself
    OAHashTable &table_;
    // The current bucket we're iterating over
    uint64_t curr_bucket_;
    // The next entry to offer
    HashEntry *curr_;
    KeyValueList *kvl_;
    uint32_t kvl_pos_;
  };

  /// Methods to begin iteration and find the end of the iterator
  Iterator begin();
  Iterator end();

 private:
  // Initialize all slots in the given list to FREE state. This is called for
  // both initialization of the hash-table and during resizing since the resized
  // array must also be initialized.
  void InitializeArray(HashEntry *entries);

  // Make a room in the key value list and return pointer to where the value
  // can be stored. This is used to add a key value pair into kv list either
  // when creating a new kv list or when inserting into an existing list
  char *StoreToKeyValueList(KeyValueList **kv_list_p);

  // If the size of the hash-table exceeds a pre-calculated threshold, it must
  // be resized. To do this, we allocate a new array, rearrange all elements in
  // the table, and replace the old array with new one.
  //
  // Note that we do not need to recompute hash values since the hash value is
  // recorded inside the entry. We use this hash value to re-probe the entry to
  // its new location. This makes key comparisons during resizing unnecessary.
  void Resize(HashEntry **entry_p_p);

  // Given a hash value, find the next free entry
  HashEntry *FindNextFreeEntry(uint64_t hash_value);

  // Return the size of key value list
  // the size is: header + values length
  uint64_t GetCurrentKeyValueListSize(uint32_t size) {
    static constexpr uint32_t kValueListHeaderSize = sizeof(KeyValueList);
    return kValueListHeaderSize + (size * value_size_);
  }

  // Does the hash-table need resizing?
  bool NeedsResize() const { return num_valid_buckets_ == resize_threshold_; }

 private:
  // XXX: Remember, if you alter any of the field below, you'll need to modify
  //      HashTableProxy. Hopefully, you'll get a compile-time error about this.

  // The buckets as a continuous chunk of storage. We use open addressing to
  // deal with hash collisions.
  HashEntry *buckets_;

  // The total number of buckets. This is used to iterate through the hash-table
  // as well as to compute current load factor.
  uint64_t num_buckets_;

  // The mask we use to find the bucket a given hash falls into.
  uint64_t bucket_mask_;

  // Total number of occupied buckets in the hash-table. Clever use of this
  // variable could terminate the iteration early when scanning the array.
  uint64_t num_valid_buckets_;

  // Total number of entries in this hash-table
  uint64_t num_entries_;

  // The threshold for resizing the hash-table. If the number of elements grows
  // to this value, we resize the table by doing reallocation and rehashing.
  uint64_t resize_threshold_;

  // The run-time size of a hash entry, which is bucket metadata overhead +
  // the size of one kv pair.
  uint64_t entry_size_;

  // Size of key itself
  uint64_t key_size_;

  // The size of the value itself
  uint64_t value_size_;
};

template <typename Key, typename Value>
void OAHashTable::Insert(uint64_t hash, const Key &key, const Value &value) {
  uint64_t bucket = hash & bucket_mask_;

  uint64_t entry_int =
      reinterpret_cast<uint64_t>(buckets_) + bucket * entry_size_;
  while (true) {
    auto *entry = reinterpret_cast<HashEntry *>(entry_int);

    // If entry is free, dump key and value
    if (entry->IsFree()) {
      // data points to key and data storage area
      char *data = StoreTuple(entry, hash);
      *reinterpret_cast<Key *>(data) = key;
      *reinterpret_cast<Value *>(data + sizeof(Key)) = value;
      return;
    }

    // If entry isn't free, check hash first
    if (entry->hash == hash) {
      // Hashes match, check key
      if (*reinterpret_cast<Key *>(entry->data) == key) {
        // data points to the value place only
        char *data = StoreTuple(entry, hash);
        *reinterpret_cast<Value *>(data) = value;
        return;
      }
    }

    // Continue
    bucket++;
    entry_int += entry_size_;

    // Wrap
    if (bucket == num_buckets_) {
      bucket = 0;
      entry_int = reinterpret_cast<uint64_t>(buckets_);
    }
  }
}

template <typename Key, typename Value>
bool OAHashTable::Probe(uint64_t hash, const Key &key,
                        std::function<void(const Value &)> &consumer) {
  uint64_t steps = 0;

  uint64_t bucket = hash & bucket_mask_;

  uint64_t entry_int =
      reinterpret_cast<uint64_t>(buckets_) + (bucket * entry_size_);

  while (steps++ < num_entries_) {
    auto *entry = reinterpret_cast<HashEntry *>(entry_int);
    // check if entry is free
    if (!entry->IsFree() && entry->hash == hash) {
      // hashes match, check key
      auto *entry_key = reinterpret_cast<Key *>(entry->data);
      if (*entry_key == key) {
        if (entry->HasKeyValueList()) {
          auto *kv_list = entry->kv_list;
          for (uint64_t pos = 0,
                        end = GetCurrentKeyValueListSize(kv_list->size);
               pos != end; pos += value_size_) {
            auto *value = reinterpret_cast<Value *>(kv_list->data + pos);
            consumer(*value);
          }
        } else {
          auto *value = reinterpret_cast<Value *>(entry->data + sizeof(Key));
          consumer(*value);
        }
        return true;
      }
    }

    // Continue
    bucket++;
    entry_int += entry_size_;

    // Wrap
    if (bucket == num_buckets_) {
      bucket = 0;
      entry_int = reinterpret_cast<uint64_t>(buckets_);
    }
  }
  return false;
}

}  // namespace util
}  // namespace codegen
}  // namespace peloton