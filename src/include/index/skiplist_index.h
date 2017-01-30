//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skiplist_index.h
//
// Identification: src/backend/index/skiplist_index.h
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
#include "type/types.h"
#include "index/index.h"

#include "index/skiplist.h"

#define SKIPLIST_INDEX_TYPE SkipListIndex <KeyType, \
                                           ValueType, \
                                           KeyComparator, \
                                           KeyEqualityChecker, \
                                           KeyHashFunc, \
                                           ValueEqualityChecker, \
                                           ValueHashFunc>

namespace peloton {
namespace index {
  
// class ItemPointerComparator {
//  public:
//   bool operator()(ItemPointer * const &p1, ItemPointer * const &p2) const {
//     return (p1->block == p2->block) && (p1->offset == p2->offset);
//   }
//   
//   ItemPointerComparator(const ItemPointerComparator&) {}
//   ItemPointerComparator() {}
// };
// 
// class ItemPointerHashFunc {
//  public:
//   size_t operator()(ItemPointer * const &p) const {
//     return std::hash<oid_t>()(p->block) ^ std::hash<oid_t>()(p->offset);
//   }
//   
//   ItemPointerHashFunc(const ItemPointerHashFunc&) {}
//   ItemPointerHashFunc() {}
// };

/**
 * Skiplist-based index implementation.
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
class SkipListIndex : public Index {
  friend class IndexFactory;

  typedef SkipList<KeyType, ValueType, KeyComparator, KeyEqualityChecker, KeyHashFunc, ValueEqualityChecker, ValueHashFunc> MapType;

  // using MapType = SkipList<KeyType,
  //                        ValueType,
  //                        KeyComparator,
  //                        KeyEqualityChecker,
  //                        KeyHashFunc,
  //                        ValueEqualityChecker,
  //                        ValueHashFunc>;

 public:
  SkipListIndex(IndexMetadata *metadata);

  ~SkipListIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer *value);

  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value);

  bool CondInsertEntry(const storage::Tuple *key,
                       ItemPointer *value,
                       std::function<bool(const void *)> predicate);

  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p);
            
  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ValueType> &result,
                 const ConjunctionScanPredicate *csp_p,
                 uint64_t limit,
                 uint64_t offset);

  void ScanAllKeys(std::vector<ValueType> &result);

  void ScanKey(const storage::Tuple *key,
               std::vector<ValueType> &result);

  std::string GetTypeName() const;

  // TODO: Implement this
  bool Cleanup() { return true; }

  // TODO: Implement this
  size_t GetMemoryFootprint() { return 0; }
  
  // TODO: Implement this
  bool NeedGC() {
    return false;
  }

  // TODO: Implement this
  void PerformGC() {
    return;
  }

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
