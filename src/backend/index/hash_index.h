//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.h
//
// Identification: src/backend/index/hash_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {
namespace index {

/**
 * Using libcuckoo for hash index
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyHasher,
          class KeyComparator, class KeyEqualityChecker>
class HashIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef cuckoohash_map<KeyType, std::vector<ValueType>, KeyHasher,
                         KeyEqualityChecker> Table;

 public:
  HashIndex(IndexMetadata *metadata);

  ~HashIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  // TODO: implement this
  bool ConditionalInsertEntry(const storage::Tuple *key __attribute__((unused)),
                              const ItemPointer &location __attribute__((unused)),
                              std::function<bool(const storage::Tuple *, const ItemPointer &)> predicate __attribute__((unused)))
                              {return true;}

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types,
                                const ScanDirectionType &scan_direction);

  std::vector<ItemPointer> ScanAllKeys();

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() {
    // TODO: implement it
    return 0;
  }

 protected:
  Table container;

  // equality checker and comparator
  KeyHasher hasher;
  KeyEqualityChecker equals;
  KeyComparator comparator;
};

}  // End index namespace
}  // End peloton namespace
