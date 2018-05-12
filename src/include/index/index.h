//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.h
//
// Identification: src/include/index/index.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/internal_types.h"
#include "common/item_pointer.h"
#include "common/logger.h"
#include "common/printable.h"
#include "type/value.h"
#include "tbb/concurrent_unordered_set.h"
#include "storage/tuple.h"

namespace peloton {

class AbstractTuple;

namespace catalog {
class Schema;
}  // namespace catalog

namespace storage {
class Tuple;
}  // namespace storage

namespace type {
class AbstractPool;
}  // namespace type

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
 public:
  IndexMetadata(std::string index_name, oid_t index_oid, oid_t table_oid,
                oid_t database_oid, IndexType index_type,
                IndexConstraintType index_constraint_type,
                const catalog::Schema *tuple_schema,
                const catalog::Schema *key_schema,
                const std::vector<oid_t> &key_attrs, bool unique_keys);

  ~IndexMetadata();

  const std::string &GetName() const { return name_; }

  oid_t GetOid() const { return index_oid; }

  oid_t GetTableOid() const { return table_oid; }

  oid_t GetDatabaseOid() const { return database_oid; }

  IndexType GetIndexType() const { return index_type_; }

  IndexConstraintType GetIndexConstraintType() const {
    return index_constraint_type_;
  }

  // Returns a schema object pointer that represents the schema of indexed
  // columns, from leading column to the least important columns.
  const catalog::Schema *GetKeySchema() const { return key_schema; }

  // Returns a schema object that represents the schema of the underlying table
  const catalog::Schema *GetTupleSchema() const { return tuple_schema; }

  // Return the number of columns inside index key (not in tuple key)
  oid_t GetColumnCount() const;

  bool HasUniqueKeys() const { return unique_keys; }

  // Returns the mapping relation between indexed columns and base table columns
  // The entry whose value is j on index i means the i-th column in the key is
  // mapped to the j-th column in the base table tuple
  const std::vector<oid_t> &GetKeyAttrs() const { return key_attrs; }

  // Returns the mapping relation between tuple key column and index key columns
  const std::vector<oid_t> &GetTupleToIndexMapping() const {
    return tuple_attrs;
  }

  double GetUtility() const { return utility_ratio; }

  void SetUtility(double p_utility_ratio) { utility_ratio = p_utility_ratio; }

  bool GetVisibility() const { return (visible_); }

  void SetVisibility(bool visible) { visible_ = visible; }

  // Get a string representation for debugging
  const std::string GetInfo() const override;

  //===--------------------------------------------------------------------===//
  // STATIC HELPERS
  //===--------------------------------------------------------------------===//

  static void SetDefaultVisibleFlag(bool flag) {
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
  virtual ~Index();

  /**
   * @brief Returns the object identifier of the index
   *
   * @return The object ID of the index
   */
  oid_t GetOid() const { return index_oid; }

  /**
   * @brief Return the metadata object associated with the index
   *
   * @return The metadata object
   */
  IndexMetadata *GetMetadata() const { return metadata; }

  /**
   * @brief Convert table column ID to index column ID
   *
   * @return The position of the tuple in the index key
   */
  oid_t TupleColumnToKeyColumn(oid_t tuple_column_id) const;

  /**
   * Insert the given key-value pair into the index. This is designed for
   * secondary indexes.
   *
   * @param key The key we want to insert into the index
   * @param value The value we want to insert into the index
   * @return True on successful insertion
   */
  virtual bool InsertEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

  /**
   * Delete the given key-value pair from the index
   *
   * @return True on successful deletion
   */
  virtual bool DeleteEntry(const storage::Tuple *key,
                           ItemPointer *location_ptr) = 0;

  /**
   * Insert the given key-value pair into the index, but only if no existing
   * values for the given key satisfy the provided predicate. In other words,
   * only perform the insert if the predicate returns false for all existing
   * values associated with the provided key.
   *
   * This function should be called for all primary/unique index insertions
   *
   * @param key The key we want to insert into the index
   * @param value The value we want to insert into the index
   * @param predicate The predicate to check on all existing values for the key
   * before performing the insertion
   * @return True on successful insertion
   */
  virtual bool CondInsertEntry(const storage::Tuple *key, ItemPointer *location,
                               std::function<bool(const void *)> predicate) = 0;

  ///////////////////////////////////////////////////////////////////
  // Index Scan
  ///////////////////////////////////////////////////////////////////

  /**
   * Scans a range inside the index and returns all elements in the result
   * vector. The type of scan to perform is specified by the given scan
   * predicate (csp_p). Whether to perform a forward or backward scan is
   * determined by the given scan_direction.
   *
   * @param value_list A list of bound key column values
   * @param tuple_column_id_list A list of column IDs to bind values to
   * @param expr_list A list of expressions used to when performing the
   * comparison of the key column and the value from the provided list
   * @param scan_direction The direction to perform the scan, either forward or
   * backward
   * @param[out] result Where the results of the scan are stored
   * @param scan_predicate The only predicate that's actually used.
   */
  virtual void Scan(const std::vector<type::Value> &value_list,
                    const std::vector<oid_t> &tuple_column_id_list,
                    const std::vector<ExpressionType> &expr_list,
                    ScanDirectionType scan_direction,
                    std::vector<ItemPointer *> &result,
                    const ConjunctionScanPredicate *scan_predicate) = 0;

  /**
   * The ScanLimit function has the same behavior as the Scan function
   * except that the scan stops after it reads offset + limit elements.
   * Therefore, maximum of limit elements are returned in the result vector.
   *
   * @param value_list A list of bound key column values
   * @param tuple_column_id_list A list of column IDs to bind values to
   * @param expr_list A list of expressions used to when performing the
   * comparison of the key column and the value from the provided list
   * @param scan_direction The direction to perform the scan, either forward or
   * backward
   * @param[out] result Where the results of the scan are stored
   * @param scan_predicate A list of conjuncts to perform additional filtering
   * @param limit How many results to actually return
   * @param offset How many items to exclude from the results
   */
  virtual void ScanLimit(const std::vector<type::Value> &value_list,
                         const std::vector<oid_t> &tuple_column_id_list,
                         const std::vector<ExpressionType> &expr_list,
                         ScanDirectionType scan_direction,
                         std::vector<ItemPointer *> &result,
                         const ConjunctionScanPredicate *csp_p, uint64_t limit,
                         uint64_t offset) = 0;

  /**
   * This is the version used to test the basic scan operation. Since it does a
   * scan planning every time it is invoked, it will be slower than the regular
   * scan. This should <b>only</b> be used for correctness testing.
   *
   * @copydoc Index::Scan()
   *
   */
  virtual void ScanTest(const std::vector<type::Value> &value_list,
                        const std::vector<oid_t> &tuple_column_id_list,
                        const std::vector<ExpressionType> &expr_list,
                        const ScanDirectionType &scan_direction,
                        std::vector<ItemPointer *> &result);

  /**
   * Scan all of the keys in the index and store their values in the result
   * vector.
   *
   * @param[out] result Where the results of the scan are stored
   */
  virtual void ScanAllKeys(std::vector<ItemPointer *> &result) = 0;

  /**
   * Finds all of the values in the index for the given key and return them in
   * the result vector. If the index is not configured to store duplicate keys,
   * then at most one value will be returned.
   *
   * @param key
   * @param[out] result Where the results of the scan are stored
   */
  virtual void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// Garbage Collection
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @brief Determine if this index needs to perform any internal garbage
   * collection
   *
   * @return True if the index needs to perform GC
   */
  virtual bool NeedGC() = 0;

  /**
   * @brief Performs one round of GC, allowing the index to clean up internal
   * data structures and reclaim unused memory.
   */
  virtual void PerformGC() = 0;

  //////////////////////////////////////////////////////////////////////////////
  /// Stats
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @brief Increase the number of tuples in the index by the provided amount
   */
  void IncreaseNumberOfTuplesBy(size_t amount);

  /**
   * @brief Decrease the number of tuples in the index by the provided amount
   */
  void DecreaseNumberOfTuplesBy(size_t amount);

  /**
   * @brief Set the number of tuples in the index
   */
  void SetNumberOfTuples(size_t num_tuples);

  /**
   * @brief Return the number of tuples in the index
   */
  size_t GetNumberOfTuples() const;

  /**
   * @brief Returns if the index is dirty. WTF that means is unknown.
   */
  bool IsDirty() const;

  /**
   * @brief No idea. Reset some dirty flag?
   */
  void ResetDirty();

  //////////////////////////////////////////////////////////////////////////////
  /// Utilities
  //////////////////////////////////////////////////////////////////////////////

  /**
   * @brief Return a string representation for the type of this index
   */
  virtual std::string GetTypeName() const = 0;

  /**
   * @brief Returns if this index only has unique keys
   */
  bool HasUniqueKeys() const { return metadata->HasUniqueKeys(); }

  oid_t GetColumnCount() const { return metadata->GetColumnCount(); }

  const std::string &GetName() const { return metadata->GetName(); }

  const catalog::Schema *GetKeySchema() const {
    return metadata->GetKeySchema();
  }

  IndexType GetIndexMethodType() const { return metadata->GetIndexType(); }

  IndexConstraintType GetIndexType() const {
    return metadata->GetIndexConstraintType();
  }

  const std::string GetInfo() const override;

  // Generic key comparator between index key and given arbitrary key
  //
  // NOTE: Do not make it static since we need the metadata fild
  bool Compare(const AbstractTuple &index_key,
               const std::vector<oid_t> &column_ids,
               const std::vector<ExpressionType> &expr_types,
               const std::vector<type::Value> &values);

  type::AbstractPool *GetPool() const { return pool; }

  void SetPopulated(bool populate){ populated = populate; }

  void ResetPopulated(){
    populated = false;
    insert_set.clear();
  }

  bool CheckDuplicate(std::pair<storage::Tuple, ItemPointer> entry){
    return(insert_set.find(entry) != insert_set.end());
  }

  /**
   * @brief Calculate the total number of bytes used by this index
   *
   * @return The number of bytes this index occupies
   */
  virtual size_t GetMemoryFootprint() = 0;

  // Get the indexed tile group offset
  virtual size_t GetIndexedTileGroupOff() {
    return indexed_tile_group_offset.load();
  }

  virtual void IncrementIndexedTileGroupOffset() {
    indexed_tile_group_offset++;
  }

 protected:
  explicit Index(IndexMetadata *schema);

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

  // variable for identifying index being populated
  bool populated = false;

  // set that records current insertions in index, used in populate index
  tbb::concurrent_unordered_set<std::pair<storage::Tuple, ItemPointer> > insert_set;
};

}  // namespace index
}  // namespace peloton
