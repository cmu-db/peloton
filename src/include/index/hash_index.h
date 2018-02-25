//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_index.h
//
// Identification: src/include/index/hash_index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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

#include "common/container/cuckoo_map.h"

/* // HASH_INDEX_TEMPLATE_ARGUMENTS
#define HASH_INDEX_TEMPLATE_ARGUMENTS                   \
  template <typename KeyType, typename ValueType, typename HashType, \
            typename PredType>

// HASH_INDEX_DEFAULT_ARGUMENTS
#define HASH_INDEX_DEFAULT_ARGUMENTS                   \
  template <typename KeyType, typename ValueType,       \
            typename HashType = DefaultHasher<KeyType>, \
            typename PredType = std::equal_to<KeyType>> */

#define HASH_INDEX_TYPE HashIndex <KeyType, ValueType, HashType, PredType>

namespace peloton {
namespace index {
  
/**
 * Hash index implementation.
 *
 * Used third party libcuckoo for underlying data structure
 */

CUCKOO_MAP_DEFAULT_ARGUMENTS
class HashIndex : public Index {
  friend class IndexFactory;

  using MapType = CUCKOO_MAP_TYPE;

 public:
  HashIndex(IndexMetadata *metadata);

  ~HashIndex();

  bool InsertEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool DeleteEntry(const storage::Tuple *key, ItemPointer *value) override;

  bool CondInsertEntry(const storage::Tuple *key,
                       ItemPointer *value,
                       std::function<bool(const void *)> predicate) override;

  void Scan(const std::vector<type::Value> &values,
            const std::vector<oid_t> &key_column_ids,
            const std::vector<ExpressionType> &expr_types,
            ScanDirectionType scan_direction,
            std::vector<ValueType> &result,
            const ConjunctionScanPredicate *csp_p) override;
            
  void ScanLimit(const std::vector<type::Value> &values,
                 const std::vector<oid_t> &key_column_ids,
                 const std::vector<ExpressionType> &expr_types,
                 ScanDirectionType scan_direction,
                 std::vector<ValueType> &result,
                 const ConjunctionScanPredicate *csp_p,
                 uint64_t limit,
                 uint64_t offset) override;

  void ScanAllKeys(std::vector<ValueType> &result) override;

  void ScanKey(const storage::Tuple *key,
               std::vector<ValueType> &result) override;

  std::string GetTypeName() const override;

  // TODO: Implement this
  size_t GetMemoryFootprint() override { return 0; }
  
  bool NeedGC() override {
    return false;
    // return container.NeedGarbageCollection();
  }

  void PerformGC() override {
    // container.PerformGarbageCollection();
    return;
  }

 protected:
  // container
  MapType container;
};

}  // namespace index
}  // namespace peloton
