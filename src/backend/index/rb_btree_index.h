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
class RBBTreeIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef stx::btree_multimap<KeyType, ValueType, KeyComparator> MapType;

  class Dummylock {
  public:
    void Lock() {}
    void Unlock() {}
  };

 public:
  RBBTreeIndex(IndexMetadata *metadata);

  ~RBBTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  // Used by RB
  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location, RBItemPointer **result);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate,
                       ItemPointer **itempointer_ptr);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate,
                       RBItemPointer **rb_itempointer_ptr);

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

  ////////////////////////////////////////////////////////////////////////
  // Used by RB
  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            const ScanDirectionType &scan_direction,
            std::vector<RBItemPointer> &);

  void ScanAllKeys(std::vector<RBItemPointer> &);

  void ScanKey(const storage::Tuple *key, std::vector<RBItemPointer> &);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<RBItemPointer *> &result);

  void ScanAllKeys(std::vector<RBItemPointer *> &result);

  void ScanKey(const storage::Tuple *key,
               std::vector<RBItemPointer *> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }

 protected:
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

  // synch helper
  // Spinlock index_lock;
  Dummylock index_lock;
};

}  // End index namespace
}  // End peloton namespace
