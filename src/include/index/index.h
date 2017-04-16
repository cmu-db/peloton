//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.h
//
// Identification: src/include/index/index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "catalog/index_catalog_object.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "type/types.h"
#include "type/value.h"

namespace peloton {

class AbstractTuple;

namespace catalog {
class Schema;
class IndexCatalogObject;
}

namespace storage {
class Tuple;
}

namespace index {

class ConjunctionScanPredicate;

/////////////////////////////////////////////////////////////////////
// Index class definition
/////////////////////////////////////////////////////////////////////

/*
 * class Index - Base class for derived indices of different types
 *
 * The index structure majorly maintains information on the schema of the schema
 * of the underlying table and the mapping relation between index key
 * and tuple key, and provides an abstracted way for the external world to
 * interact with the underlying index implementation without exposing
 * the actual implementation's interface.
 *
 * Index object also handles predicate scan, in addition to simple insert,
 * delete, predicate insert, point query, and full index scan. Predicate scan
 * only supports conjunction, and may or may not be optimized depending on
 * the type of expressions inside the predicate.
 */
class Index : public Printable {
  friend class IndexFactory;

 public:
  /*
   * GetOid() - Returns the object identifier of the index
   *
   * Note that there is also an index oid stored inside the index_catalog_object. These
   * two must remain the same and actually the one stored in index object
   * is copied from the index_catalog_object
   */
  oid_t GetOid() const { return index_oid; }

  // Return the index_catalog_object object associated with the index
  catalog::IndexCatalogObject *GetIndexCatalogObject() const { return index_catalog_object; }

  // Convert table column ID to index column ID
  oid_t TupleColumnToKeyColumn(oid_t tuple_column_id) const;

  virtual ~Index();

  ///////////////////////////////////////////////////////////////////
  // Point Modification
  ///////////////////////////////////////////////////////////////////

  // designed for secondary indexes.
  virtual bool InsertEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

  // delete the index entry linked to given tuple and location
  virtual bool DeleteEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

  // First retrieve all Key-Value pairs of the given key
  // Return false if any of those k-v pairs satisfy the predicate
  // If not any of those k-v pair satisfy the predicate, insert the k-v pair
  // into the index and return true.
  // This function should be called for all primary/unique index insert
  virtual bool CondInsertEntry(const storage::Tuple *key, ItemPointer *location,
                               std::function<bool(const void *)> predicate) = 0;

  ///////////////////////////////////////////////////////////////////
  // Index Scan
  ///////////////////////////////////////////////////////////////////

  virtual void Scan(const std::vector<type::Value> &value_list,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    ScanDirectionType scan_direction,
                    std::vector<ItemPointer *> &result,
                    const ConjunctionScanPredicate *csp_p) = 0;

  virtual void ScanLimit(const std::vector<type::Value> &value_list,
                         const std::vector<oid_t> &tuple_column_id_list,
                         const std::vector<ExpressionType> &expr_list,
                         ScanDirectionType scan_direction,
                         std::vector<ItemPointer *> &result,
                         const ConjunctionScanPredicate *csp_p, uint64_t limit,
                         uint64_t offset) = 0;

  // This is the version used to test scan
  // Since it does scan planning everytime, it is slow, and should
  // only be used for correctness testing
  virtual void ScanTest(const std::vector<type::Value> &value_list,
                        const std::vector<oid_t> &tuple_column_id_list,
                        const std::vector<ExpressionType> &expr_list,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer *> &result);

  virtual void ScanAllKeys(std::vector<ItemPointer *> &result) = 0;

  virtual void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) = 0;

  ///////////////////////////////////////////////////////////////////
  // Garbage Collection
  ///////////////////////////////////////////////////////////////////

  // This gives a hint on whether GC is needed on the index
  // For those that do not need GC this always return false
  virtual bool NeedGC() = 0;

  // This function performs one round of GC
  // For those that do not need GC this should return immediately
  virtual void PerformGC() = 0;

  // The following two are used to perform GC in a
  // fast manner
  // Because if we register for epoch for every operation then
  // essentially we are synchronizing on each operation which does
  // not scale at all
  // virtual void *JoinEpoch() = 0;
  // virtual void LeaveEpoch(void *) = 0;

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const size_t amount);

  void DecreaseNumberOfTuplesBy(const size_t amount);

  void SetNumberOfTuples(const size_t num_tuples);

  size_t GetNumberOfTuples() const;

  bool IsDirty() const;

  void ResetDirty();

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  virtual std::string GetTypeName() const = 0;

  /**
   * Currently, UniqueIndex is just an index with additional checks.
   * We might have to make a different class in future for maximizing
   * performance of UniqueIndex.
   */
  bool HasUniqueKeys() const { return index_catalog_object->HasUniqueKeys(); }

  oid_t GetColumnCount() const { return index_catalog_object->GetColumnCount(); }

  const std::string &GetName() const { return index_catalog_object->GetName(); }

  const catalog::Schema *GetKeySchema() const {
    return index_catalog_object->GetKeySchema();
  }

  IndexType GetIndexMethodType() { return index_catalog_object->GetIndexType(); }

  IndexConstraintType GetIndexType() const {
    return index_catalog_object->GetIndexConstraintType();
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Generic key comparator between index key and given arbitrary key
  //
  // NOTE: Do not make it static since we need the index_catalog_object field
  bool Compare(const AbstractTuple &index_key,
               const std::vector<oid_t> &column_ids,
               const std::vector<ExpressionType> &expr_types,
               const std::vector<type::Value> &values);

  type::AbstractPool *GetPool() const { return pool; }

  // Get the memory footprint
  virtual size_t GetMemoryFootprint() = 0;

  // Get the indexed tile group offset
  virtual size_t GetIndexedTileGroupOff() {
    return indexed_tile_group_offset.load();
  }

  virtual void IncrementIndexedTileGroupOffset() {
    indexed_tile_group_offset++;

    return;
  }

 protected:
  Index(catalog::IndexCatalogObject *index_catalog_object);

  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//

  catalog::IndexCatalogObject *index_catalog_object;

  oid_t index_oid = INVALID_OID;

  // access counters
  int lookup_counter;
  int insert_counter;
  int delete_counter;
  int update_counter;

  // number of tuples
  size_t number_of_tuples = 0;

  // dirty flag
  bool dirty = false;

  // pool
  type::AbstractPool *pool = nullptr;

  // This is used by index tuner
  std::atomic<size_t> indexed_tile_group_offset;
};

}  // End index namespace
}  // End peloton namespace
