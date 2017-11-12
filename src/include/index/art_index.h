//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// copy_executor.cpp
//
// Identification: src/include/index/art_index.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "catalog/manager.h"
#include "common/platform.h"
#include "type/types.h"
#include "index/index.h"

#include "index/art.h"

namespace peloton {
namespace index {

class ArtIndex : public Index {
  friend class IndexFactory;

public:
  ArtIndex();
  ArtIndex(IndexMetadata *metadata);
  ArtIndex(IndexMetadata *metadata, AdaptiveRadixTree::LoadKeyFunction loadKeyForTest);
  ~ArtIndex();


  // designed for secondary indexes.
  bool InsertEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr);

  // delete the index entry linked to given tuple and location
  bool DeleteEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr);

  // First retrieve all Key-Value pairs of the given key
  // Return false if any of those k-v pairs satisfy the predicate
  // If not any of those k-v pair satisfy the predicate, insert the k-v pair
  // into the index and return true.
  // This function should be called for all primary/unique index insert
  bool CondInsertEntry(const storage::Tuple *key, ItemPointer *location,
                               std::function<bool(const void *)> predicate);



  ///////////////////////////////////////////////////////////////////
  // Index Scan
  ///////////////////////////////////////////////////////////////////

  void Scan(const std::vector<type::Value> &value_list,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    ScanDirectionType scan_direction,
                    std::vector<ItemPointer *> &result,
                    const ConjunctionScanPredicate *csp_p);

  void ScanLimit(const std::vector<type::Value> &value_list,
                         const std::vector<oid_t> &tuple_column_id_list,
                         const std::vector<ExpressionType> &expr_list,
                         ScanDirectionType scan_direction,
                         std::vector<ItemPointer *> &result,
                         const ConjunctionScanPredicate *csp_p, uint64_t limit,
                         uint64_t offset);


  void ScanAllKeys(std::vector<ItemPointer *> &result);

  void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result);

  // TODO: Implement this
  size_t GetMemoryFootprint() { return 0; }

  // TODO: Implement this
  bool NeedGC() { return false; }

  // TODO: Implement this
  void PerformGC() { return; }

  std::string GetTypeName() const;

  static void WriteValueInBytes(type::Value value, char *c, int offset, int length);

  static void WriteIndexedAttributesInKey(const storage::Tuple *tuple, ARTKey& index_key);

public:
  int KeyType;

  AdaptiveRadixTree art_;

};
}
}
