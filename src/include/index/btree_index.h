//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// btree_index.h
//
// Identification: src/include/index/btree_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <map>
#include <string>

#include "catalog/manager.h"
#include "common/platform.h"
#include "common/types.h"
#include "index/index.h"

#include "stx/btree_multimap.h"
#include "index/scan_optimizer.h"

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

  bool InsertEntry(const storage::Tuple *key, ItemPointer *value);

  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value);

  bool CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate);

  void Scan(const std::vector<common::Value> &value_list,
            const std::vector<oid_t> &tuple_column_id_list,
            const std::vector<ExpressionType> &expr_list,
            const ScanDirectionType &scan_direction,
            std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p);

  void ScanAllKeys(std::vector<ValueType> &result);

  void ScanKey(const storage::Tuple *key, std::vector<ValueType> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }
  
  bool NeedGC() {
    return false;
  }

  void PerformGC() {
    return;
  }

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
