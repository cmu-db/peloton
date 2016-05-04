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

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer> &);

  void ScanAllKeys(std::vector<ItemPointer> &);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer> &);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer *> &result);

  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key,
               std::vector<ItemPointer *> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }

  oid_t GetIndexedTileGroupOff() {
    oid_t ret = INVALID_OID;

//    index_lock.ReadLock();
    ret = indexed_tile_group_offset_;
//    index_lock.Unlock();

    return ret;
  }

  void IncreamentIndexedTileGroupOff() {
//    index_lock.WriteLock();

    if (indexed_tile_group_offset_ == INVALID_OID) {
      indexed_tile_group_offset_ = START_OID;
    } else {
      indexed_tile_group_offset_++;
    }

//    index_lock.Unlock();
  }

 protected:
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // synch helper
  RWLock index_lock;

  oid_t indexed_tile_group_offset_ = INVALID_OID;
};

}  // End index namespace
}  // End peloton namespace
