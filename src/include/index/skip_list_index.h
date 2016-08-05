//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// skip_list_index.h
//
// Identification: src/include/index/skip_list_index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <vector>
#include <map>
#include <string>

#include "catalog/manager.h"
#include "common/types.h"
#include "index/index.h"

#include "container/skip_list_map.h"

namespace peloton {
namespace index {

/**
 * Skip-list based index
 *
 * @see Index
 */
template <typename KeyType, typename ValueType, class KeyComparator,
          class KeyEqualityChecker>
class SkipListIndex : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef SkipListMap<KeyType, ValueType, KeyComparator> MapType;

 public:
  SkipListIndex(IndexMetadata *metadata);

  ~SkipListIndex();

  bool InsertEntry(const storage::Tuple *key, const ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool CondInsertEntry(const storage::Tuple *key, const ItemPointer &location,
                       std::function<bool(const ItemPointer &)> predicate);

  void Scan(const std::vector<Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &exprs,
            const ScanDirectionType &scan_direction,
            std::vector<ItemPointer *> &result);

  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key, std::vector<ItemPointer *> &result);

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return 0; }
  
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
};

}  // End index namespace
}  // End peloton namespace
