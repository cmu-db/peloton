//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// btree_index.h
//
// Identification: src/backend/index/btree_index.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"

#include "backend/index/bwtree.h"

namespace peloton {
namespace index {

/**
 * BW tree-based index implementation.
 *
 * @see Index
 */

static const char IS_REGULAR = 0x00;
static const char IS_NEG_INF = 0x01;
static const char IS_POS_INF = 0x10;

template <typename KeyType>
struct BoundedKey {
  static const BoundedKey<KeyType> NEG_INF_KEY;
  static const BoundedKey<KeyType> POS_INF_KEY;

  BoundedKey();
  BoundedKey(char key_type);
  BoundedKey(KeyType key);

  // Maybe we want word alignment? Ignoring for now
  char key_type;
  KeyType key;
};

template <typename KeyType, class KeyComparator>
struct BoundedKeyComparator {
  BoundedKeyComparator(KeyComparator m_key_less);

  bool operator()(const BoundedKey<KeyType> &l,
                  const BoundedKey<KeyType> &r) const;

  const KeyComparator m_key_less;
};

template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  typedef BWTree<BoundedKey<KeyType>, ValueType,
                 BoundedKeyComparator<KeyType, KeyComparator>> MapType;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer location);

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types,
                                const ScanDirectionType &scan_direction);

  std::vector<ItemPointer> ScanAllKeys();

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key);

  std::string GetTypeName() const;

  // Do nothing on this the garbage collection is doing its work
  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.getMemoryFootprint(); }

 protected:
  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // container
  MapType container;

  // synch helper
  RWLock index_lock;
};

}  // End index namespace
}  // End peloton namespace
