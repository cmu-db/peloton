//
// Created by Min Huang on 9/18/17.
//

#pragma once

#include "index/art.h"

namespace peloton {
namespace index {

class ArtIndex : public index {
  friend class IndexFactory;

public:
  ArtIndex();
  ~ArtIndex();


  // designed for secondary indexes.
  bool InsertEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

  // delete the index entry linked to given tuple and location
  bool DeleteEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

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


protected:
  int KeyType;

  Tree artTree;

};
}
}
