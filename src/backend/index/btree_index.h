//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// btree_index.h
//
// Identification: src/backend/index/btree_index.h
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

#include "stx/btree_multimap.h"

namespace peloton {
namespace index {

/**
 * STX B+tree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
class BTreeIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef stx::btree_multimap<KeyType, ValueType, KeyComparator> MapType;

 public:
  BTreeIndex(IndexMetadata *metadata);

  ~BTreeIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool ConditionalInsertEntry(const storage::Tuple *key,
      const ItemPointer &location,
      std::function<bool(const storage::Tuple *, const ItemPointer &)> predicate);

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types,
                                const ScanDirectionType &scan_direction);

  std::vector<ItemPointer> ScanAllKeys();

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }

 protected:
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // synch helper
  RWLock index_lock;
};

}  // End index namespace
}  // End peloton namespace
