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

namespace peloton {
namespace index {

/**
 * BwTree-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, typename KeyComparator,
          typename KeyEqualityChecker>
class BwTreeIndex : public Index {
  friend class IndexFactory;

  typedef BwTree<KeyType, ValueType, KeyComparator, KeyEqualityChecker> MapType;

 public:
  BwTreeIndex(IndexMetadata *metadata);

  ~BwTreeIndex();

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

  // TODO: Implement this
  bool Cleanup() { return true; }

  // TODO: Implement this
  size_t GetMemoryFootprint() { return 0; }

 protected:
  // container
  MapType container;

  // equality checker and comparator
  KeyEqualityChecker equals;
  KeyComparator comparator;

};

}  // End index namespace
}  // End peloton namespace