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

#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/abstract_pool.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {

class AbstractTuple;

namespace catalog {
class Schema;
}

namespace storage {
class Tuple;
}

namespace index {

class ConjunctionScanPredicate;

/////////////////////////////////////////////////////////////////////
// IndexMetadata class definition
/////////////////////////////////////////////////////////////////////

/*
 * class IndexMetadata - Holds metadata of an index object
 *
 * The metadata object maintains the tuple schema and key schema of an
 * index, since the external callers does not know the actual structure of
 * the index key, so it is the index's responsibility to maintain such a
 * mapping relation and does the conversion between tuple key and index key
 */
class IndexMetadata : public Printable {
  IndexMetadata() = delete;

 public:
  IndexMetadata(std::string index_name, oid_t index_oid, oid_t table_oid,
                oid_t database_oid, IndexType index_type,
                IndexConstraintType index_constraint_type,
                const catalog::Schema *tuple_schema,
                const catalog::Schema *key_schema,
                const std::vector<oid_t> &key_attrs, bool unique_keys);

  ~IndexMetadata();

  const std::string &GetName() const { return name_; }

  inline oid_t GetOid() { return index_oid; }

  inline oid_t GetTableOid() { return table_oid; }

  inline oid_t GetDatabaseOid() { return database_oid; }

  inline IndexType GetIndexType() { return index_type_; }

  IndexConstraintType GetIndexConstraintType() {
    return index_constraint_type_;
  }

  /*
   * Returns a schema object pointer that represents
   * the schema of indexed columns, from leading column
   * to the least important columns
   */
  inline const catalog::Schema *GetKeySchema() const { return key_schema; }

  /*
   * Returns a schema object pointer that represents
   * the schema of the underlying table
   */
  inline const catalog::Schema *GetTupleSchema() const { return tuple_schema; }

  // Return the number of columns inside index key (not in tuple key)
  //
  // Note that this must be defined inside the cpp source file
  // because it uses the member of catalog::Schema which is not known here
  oid_t GetColumnCount() const;

  bool HasUniqueKeys() const { return unique_keys; }

  /*
   * GetKeyAttrs() - Returns the mapping relation between indexed columns
   *                 and base table columns
   *
   * The return value is a const vector reference
   *
   * The entry whose value is j on index i means the i-th column in the
   * key is mapped to the j-th column in the base table tuple
   */
  inline const std::vector<oid_t> &GetKeyAttrs() const { return key_attrs; }

  /*
   * GetTupleToIndexMapping() - Returns the mapping relation between tuple key
   *                            column and index key columns
   */
  inline const std::vector<oid_t> &GetTupleToIndexMapping() const {
    return tuple_attrs;
  }

  inline double GetUtility() const { return utility_ratio; }

  inline void SetUtility(double p_utility_ratio) {
    utility_ratio = p_utility_ratio;
  }

  inline bool GetVisibility() const { return (visible_); }

  inline void SetVisibility(bool visibile) { visible_ = visibile; }

  /*
   * GetInfo() - Get a string representation for debugging
   */
  const std::string GetInfo() const;

  //===--------------------------------------------------------------------===//
  // STATIC HELPERS
  //===--------------------------------------------------------------------===//

  static inline void SetDefaultVisibleFlag(bool flag) {
    LOG_DEBUG("Set IndexMetadata visible flag to '%s'",
              (flag ? "true" : "false"));
    index_default_visibility = flag;
  }

  ///////////////////////////////////////////////////////////////////
  // IndexMetadata Data Member Definition
  ///////////////////////////////////////////////////////////////////

  // deprecated, use catalog::IndexCatalog::GetInstance()->GetIndexName()
  std::string name_;

  const oid_t index_oid;
  const oid_t table_oid;
  const oid_t database_oid;

  const IndexType index_type_;

  const IndexConstraintType index_constraint_type_;

  // schema of the indexed base table
  const catalog::Schema *tuple_schema;

  // schema of the index key which is a reordered subset
  // of the underlying table schema
  const catalog::Schema *key_schema;

 private:
  // The mapping relation between key schema and tuple schema
  const std::vector<oid_t> key_attrs;

  // The mapping relation between tuple schema and key schema
  // i.e. if the column in tuple is not indexed, then it is set to INVALID_OID
  //      if the column in tuple is indexed, then it is the index in index_key
  //
  // This vector has the same length as the tuple schema.columns
  std::vector<oid_t> tuple_attrs;

  // Whether keys are unique (e.g. primary key)
  const bool unique_keys;

  // utility of an index
  double utility_ratio = INVALID_RATIO;

  // If set to true, then this index is visible to the planner
  bool visible_;

  // This is a magic flag that tells us whether new
  static bool index_default_visibility;
};

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
   * Note that there is also an index oid stored inside the metadata. These
   * two must remain the same and actually the one stored in index object
   * is copied from the index metadata
   */
  oid_t GetOid() const { return index_oid; }

  // Return the metadata object associated with the index
  IndexMetadata *GetMetadata() const { return metadata; }

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
  bool HasUniqueKeys() const { return metadata->HasUniqueKeys(); }

  oid_t GetColumnCount() const { return metadata->GetColumnCount(); }

  const std::string &GetName() const { return metadata->GetName(); }

  const catalog::Schema *GetKeySchema() const {
    return metadata->GetKeySchema();
  }

  IndexType GetIndexMethodType() { return metadata->GetIndexType(); }

  IndexConstraintType GetIndexType() const {
    return metadata->GetIndexConstraintType();
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Generic key comparator between index key and given arbitrary key
  //
  // NOTE: Do not make it static since we need the metadata fild
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
  Index(IndexMetadata *schema);

  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//

  IndexMetadata *metadata;

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

}  // namespace index
}  // namespace peloton
