//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index.h
//
// Identification: src/index/skiplist_index.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <map>

#include "catalog/manager.h"
#include "common/platform.h"
#include "common/internal_types.h"
#include "index/index.h"

#include "index/skiplist.h"

#define SKIPLIST_INDEX_TYPE                                            \
  SkipListIndex<KeyType, ValueType, KeyComparator, KeyEqualityChecker, \
                ValueEqualityChecker>

namespace peloton {
namespace index {

/**
 * Skiplist-based index implementation.
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, typename KeyComparator,
    typename KeyEqualityChecker, typename ValueEqualityChecker>
class SkipListIndex : public Index {
  friend class IndexFactory;

  // typedef SkipList<KeyType, ValueType, KeyComparator, KeyEqualityChecker,
  // ValueEqualityChecker> MapType;

  using MapType = SkipList<KeyType, ValueType, KeyComparator,
                           KeyEqualityChecker, ValueEqualityChecker>;

 public:
  SkipListIndex(IndexMetadata *metadata);

  ~SkipListIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool CondInsertEntry(const storage::Tuple *key, ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;

  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction, std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p) override;

  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ValueType> &result,
                 const ConjunctionScanPredicate *csp_p, uint64_t limit,
                 uint64_t offset) override;

  void ScanAllKeys(std::vector<ValueType> &result) override;

  void ScanKey(const storage::Tuple *key, std::vector<ValueType> &result) override;

  std::string GetTypeName() const override;

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }

  bool NeedGC() { return container.NeedGC(); }

  void PerformGC() {
    container.PerformGC();
    return;
  }

 protected:
  // equality checker and comparator
  KeyComparator comparator;
  KeyEqualityChecker equals;

  // container
  MapType container;
};

}  // namespace index
}  // namespace peloton
