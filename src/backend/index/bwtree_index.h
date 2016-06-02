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

#include "backend/catalog/manager.h"
#include "backend/common/platform.h"
#include "backend/common/types.h"
#include "backend/index/index.h"

#include "backend/index/bwtree.h"

#define BWTREE_INDEX_TYPE BWTreeIndex <KeyType, \
                                       ValueType, \
                                       KeyComparator, \
                                       KeyEqualityChecker, \
                                       ValueEqualityChecker, \
                                       ValueHashFunc>

namespace peloton {
namespace index {

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
          typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>,
          typename ValueHashFunc = std::hash<ValueType>>
class BWTreeIndex : public Index {
  friend class IndexFactory;

  using MapType = BwTree<KeyType,
                         ValueType,
                         KeyComparator,
                         KeyEqualityChecker,
                         ValueEqualityChecker,
                         ValueHashFunc>;

 public:
  BWTreeIndex(IndexMetadata *metadata);

  ~BWTreeIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate,
                       ItemPointer **itemptr_ptr);

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
  
  // container
  MapType container;

  // synch helper
  RWLock index_lock;
};

}  // End index namespace
}  // End peloton namespace
