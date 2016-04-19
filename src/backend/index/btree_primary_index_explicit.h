//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// btree_index.h
//
// Identification: src/backend/index/btree_primary_index.h
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
class BTreePrimaryIndexExplicit : public Index {
  friend class IndexFactory;

  // Define the container type
  typedef stx::btree_multimap<KeyType, ValueType, KeyComparator> MapType;

 public:
  BTreePrimaryIndexExplicit(IndexMetadata *metadata);

  ~BTreePrimaryIndexExplicit();

  bool InsertEntry(const storage::Tuple *key, ItemPointer &location);

  bool DeleteEntry(const storage::Tuple *key, const ItemPointer &location);

  bool UpdateEntry(const storage::Tuple *key, const ItemPointer &location);

  bool ConditionalInsertEntry(const storage::Tuple *key,
      const ItemPointer &location,
      std::function<bool(const storage::Tuple *, const ItemPointer &)> predicate);

  // std::vector<ItemPointer> Scan(const std::vector<Value> &values __attribute__((unused)),
  //                               const std::vector<oid_t> &key_column_ids __attribute__((unused)),
  //                               const std::vector<ExpressionType> &expr_types __attribute__((unused)),
  //                               const ScanDirectionType &scan_direction __attribute__((unused))) { }

  // std::vector<ItemPointer> ScanAllKeys() { }

  // std::vector<ItemPointer> ScanKey(const storage::Tuple *key __attribute__((unused))) { }

  void Scan(
      const std::vector<Value> &values __attribute__((unused)),
      const std::vector<oid_t> &key_column_ids __attribute__((unused)),
      const std::vector<ExpressionType> &exprs __attribute__((unused)),
      const ScanDirectionType &scan_direction __attribute__((unused)),
      std::vector<std::shared_ptr<ItemPointerHeader>> &result __attribute__((unused)));

  void ScanAllKeys(std::vector<std::shared_ptr<ItemPointerHeader>> &result);

  void ScanKey(const storage::Tuple *key, std::vector<std::shared_ptr<ItemPointerHeader>> &result __attribute__((unused)));

  std::string GetTypeName() const;

  bool Cleanup() { return true; }

  size_t GetMemoryFootprint() { return container.GetMemoryFootprint(); }

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
