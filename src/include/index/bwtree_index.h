//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bwtree_index.h
//
// Identification: src/backend/index/bwtree_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "catalog/manager.h"
#include "common/platform.h"
#include "common/types.h"
#include "index/index.h"

#include "index/bwtree.h"

#define BWTREE_INDEX_TYPE BWTreeIndex <KeyType, \
                                       ValueType, \
                                       KeyComparator, \
                                       KeyEqualityChecker, \
                                       KeyHashFunc, \
                                       ValueEqualityChecker, \
                                       ValueHashFunc>

namespace peloton {
namespace index {
  
class ItemPointerComparator {
 public:
  bool operator()(ItemPointer * const &p1, ItemPointer * const &p2) const {
    return (p1->block == p2->block) && (p1->offset == p2->offset);
  }
  
  ItemPointerComparator(const ItemPointerComparator&) {}
  ItemPointerComparator() {}
};

class ItemPointerHashFunc {
 public:
  size_t operator()(ItemPointer * const &p) const {
    return std::hash<oid_t>()(p->block) ^ std::hash<oid_t>()(p->offset);
  }
  
  ItemPointerHashFunc(const ItemPointerHashFunc&) {}
  ItemPointerHashFunc() {}
};

/**
 * BW tree-based index implementation.
 *
 * NOTE: The template argument list has two more entries, since
 * BwTree is implemented as a multimap, potentially there could
 * be many values mapped by a single key, and we need to distinguish
 * between values.
 *
 * @see Index
 */
template <typename KeyType,
          typename ValueType,
          typename KeyComparator,
          typename KeyEqualityChecker,
          typename KeyHashFunc,
          typename ValueEqualityChecker,
          typename ValueHashFunc>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  using MapType = BwTree<KeyType,
                         ValueType,
                         KeyComparator,
                         KeyEqualityChecker,
                         KeyHashFunc,
                         ValueEqualityChecker,
                         ValueHashFunc>;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key,
                       const ItemPointer &location,
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
            const std::vector<ExpressionType> &expr_types,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer *> &result);

  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key,
               std::vector<ItemPointer *> &result);

  std::string GetTypeName() const;

  // TODO: Implement this
  bool Cleanup() { return true; }

  // TODO: Implement this
  size_t GetMemoryFootprint() { return 0; }

 protected:
  // equality checker and comparator
  KeyComparator comparator;
  KeyEqualityChecker equals;
  KeyHashFunc hash_func;
  
  // container
  MapType container;
};

}  // End index namespace
}  // End peloton namespace
