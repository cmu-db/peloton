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

#include "stx/btree_multimap.h"
#include "backend/catalog/manager.h"
#include "backend/common/types.h"
#include "backend/common/synch.h"
#include "backend/index/index.h"

namespace peloton {
namespace index {

/**
 * STX B+tree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, class KeyComparator, class KeyEqualityChecker>
class BtreeIndex : public Index {
  friend class IndexFactory;

  typedef ItemPointer ValueType;

  typedef stx::btree_multimap<KeyType, ValueType, KeyComparator> MapType;
  // typedef std::multimap<KeyType, ValueType, KeyComparator> MapType;

 public:
  BtreeIndex(IndexMetadata *metadata);

  ~BtreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer location);

  std::vector<ItemPointer> Scan(const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types);

  std::vector<ItemPointer> ScanAllKeys();

  std::vector<ItemPointer> ScanKey(const storage::Tuple *key);

  std::string GetTypeName() const;

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
