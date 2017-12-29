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
#include "common/internal_types.h"
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
  size_t GetMemoryFootprint() { return 0; }
  
  bool NeedGC() {
    return container.NeedGarbageCollection();
  }

  void PerformGC() {
    container.PerformGarbageCollection();
    
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

}  // namespace index
}  // namespace peloton
