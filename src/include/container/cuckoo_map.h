//===----------------------------------------------------------------------===//
//
//       Peloton
//
// cuckoo_map.h
//
// Identification: src/include/common/cuckoo_map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "libcds/cds/opt/hash.h"
#include "libcds/cds/container/cuckoo_map.h"

namespace peloton {

// CUCKOO_MAP_TEMPLATE_ARGUMENTS
#define CUCKOO_MAP_TEMPLATE_ARGUMENTS template <typename KeyType, \
    typename ValueType>

// CUCKOO_MAP_TYPE
#define CUCKOO_MAP_TYPE CuckooMap<KeyType, ValueType>

CUCKOO_MAP_TEMPLATE_ARGUMENTS
class CuckooMap {
 public:

  CuckooMap();
  ~CuckooMap();

  // Inserts a item
  bool Insert(const KeyType &key, ValueType value);

  // Extracts item with high priority
  std::pair<bool, bool> Update(const KeyType &key,
                               ValueType value,
                               bool bInsert=true);

  // Extracts the corresponding value
  bool Find(const KeyType &key, ValueType& value);

  // Delete key from the cuckoo_map
  bool Erase(const KeyType &key);

  // Checks whether the cuckoo_map contains key
  bool Contains(const KeyType &key);

  // Clears the tree (thread safe, not atomic)
  void Clear();

  // Returns item count in the cuckoo_map
  size_t GetSize() const;

  // Checks if the cuckoo_map is empty
  bool IsEmpty() const;

 private:

  struct hash1 {
      size_t operator()(KeyType const& s) const {
          return cds::opt::v::hash<KeyType>()(s);
      }
  };

  struct hash2: private hash1 {
      size_t operator()(KeyType const& s) const {
          size_t h = ~( hash1::operator()(s));
          return ~h + 0x9e3779b9 + (h << 6) + (h >> 2);
      }
  };

  struct cuckoo_map_traits: public cds::container::cuckoo::traits {
    typedef std::equal_to<KeyType> equal_to;
    typedef cds::opt::hash_tuple< hash1, hash2 >  hash;

    static bool const store_hash = true;
  };

  // cuckoo map
  typedef cds::container::CuckooMap<KeyType, ValueType, cuckoo_map_traits> cuckoo_map_t;

  // map pair <key, value>
  typedef typename cuckoo_map_t::value_type map_pair;

  cuckoo_map_t cuckoo_map;
};

}  // namespace peloton
