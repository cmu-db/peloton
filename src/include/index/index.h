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

#include <vector>
#include <string>
#include <functional>
#include <memory>
#include <set>
#include <atomic>

#include "common/printable.h"
#include "common/types.h"

namespace peloton {

class AbstractTuple;
class VarlenPool;

namespace catalog {
class Schema;
}

namespace storage {
class Tuple;
}

namespace index {

//===--------------------------------------------------------------------===//
// IndexMetadata
//===--------------------------------------------------------------------===//

/**
 * Parameter for constructing Index. catalog::Schema, then key schema
 */
class IndexMetadata : public Printable {
  IndexMetadata() = delete;

 public:
  IndexMetadata(std::string index_name,
                oid_t index_oid,
                IndexType method_type,
                IndexConstraintType index_type,
                const catalog::Schema *tuple_schema,
                const catalog::Schema *key_schema,
                const std::vector<oid_t>& key_attrs,
                bool unique_keys)
 : index_name(index_name),
   index_oid(index_oid),
   method_type(method_type),
   index_type(index_type),
   tuple_schema(tuple_schema),
   key_schema(key_schema),
   key_attrs(key_attrs),
   unique_keys(unique_keys) {}

  ~IndexMetadata();

  const std::string &GetName() const { return index_name; }

  oid_t GetOid() { return index_oid; }

  IndexType GetIndexMethodType() { return method_type; }

  IndexConstraintType GetIndexType() { return index_type; }

  const catalog::Schema *GetKeySchema() const { return key_schema; }

  oid_t GetColumnCount() const;

  bool HasUniqueKeys() const { return unique_keys; }

  std::vector<oid_t> GetKeyAttrs() const { return key_attrs; }

  double GetUtility() const { return utility_ratio; }

  void SetUtility(double utility_ratio_) {
    utility_ratio = utility_ratio_;
  }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  std::string index_name;

  oid_t index_oid;

  IndexType method_type;

  IndexConstraintType index_type;

  // schema of tuple values
  const catalog::Schema *tuple_schema;

  // schema of keys
  const catalog::Schema *key_schema;

  // key attributes
  std::vector<oid_t> key_attrs;

  // unique keys ?
  bool unique_keys;

  // utility of an index
  double utility_ratio = INVALID_RATIO;
};

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

/**
 * Index on a table maps from key value to tuple pointers.
 *
 * @see IndexFactory
 */
class Index : public Printable {
  friend class IndexFactory;

 public:
  oid_t GetOid() const { return index_oid; }

  IndexMetadata *GetMetadata() const { return metadata; }

  virtual ~Index();

  //===--------------------------------------------------------------------===//
  // Mutators
  //===--------------------------------------------------------------------===//

  // insert an index entry linked to given tuple
  virtual bool InsertEntry(const storage::Tuple *key,
                           const ItemPointer &location) = 0;

  // delete the index entry linked to given tuple and location
  virtual bool DeleteEntry(const storage::Tuple *key,
                           const ItemPointer &location) = 0;

  // First retrieve all Key-Value pairs of the given key
  // Return false if any of those k-v pairs satisfy the predicate
  // If not any of those k-v pair satisfy the predicate, insert the k-v pair
  // into the index and return true.
  // This function should be called for all primary/unique index insert
  virtual bool CondInsertEntry(
      const storage::Tuple *key, const ItemPointer &location,
      std::function<bool(const ItemPointer &)> predicate) = 0;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  virtual void Scan(const std::vector<Value> &values,
                    const std::vector<oid_t> &key_column_ids,
                    const std::vector<ExpressionType> &exprs,
                    const ScanDirectionType &scan_direction,
                    std::vector<ItemPointer *> &result) = 0;

  virtual void ScanAllKeys(std::vector<ItemPointer *> &result) = 0;

  virtual void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) = 0;

  // This gives a hint on whether GC is needed on the index
  // for those that do not need GC this always return false
  virtual bool NeedGC() = 0;
  
  // This function performs one round of GC
  // For those that do not need GC this should return immediately
  virtual void PerformGC() = 0;

  // The following two are used to perform GC in a 
  // fast manner
  // Because if we register for epoch for every operation then
  // essentially we are synchronizing on each operation which does
  // not scale at all
  //virtual void *JoinEpoch() = 0;
  //virtual void LeaveEpoch(void *) = 0;

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

  IndexType GetIndexMethodType() { return metadata->GetIndexMethodType(); }

  IndexConstraintType GetIndexType() const { return metadata->GetIndexType(); }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  // Generic key comparator between index key and given arbitrary key
  static bool Compare(const AbstractTuple &index_key,
                      const std::vector<oid_t> &column_ids,
                      const std::vector<ExpressionType> &expr_types,
                      const std::vector<Value> &values);

  VarlenPool *GetPool() const { return pool; }

  // Garbage collect
  virtual bool Cleanup() = 0;

  // Get the memory footprint
  virtual size_t GetMemoryFootprint() = 0;

  bool static ValuePairComparator(const std::pair<peloton::Value, int> &i,
                                  const std::pair<peloton::Value, int> &j);

  // Get the indexed tile group offset
  virtual size_t GetIndexedTileGroupOff() {
    return indexed_tile_group_offset_.load();
  }

  virtual void IncrementIndexedTileGroupOffset() {
    indexed_tile_group_offset_++;
    return;
  }

 protected:
  Index(IndexMetadata *schema);

  // Set the lower bound tuple for index iteration
  bool ConstructLowerBoundTuple(storage::Tuple *index_key,
                                const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types);

  bool IfForwardExpression(ExpressionType e);

  bool IfBackwardExpression(ExpressionType e);

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
  size_t number_of_tuples;

  // dirty flag
  bool dirty = false;

  // pool
  VarlenPool *pool = nullptr;

  std::atomic<size_t> indexed_tile_group_offset_;
};

}  // End index namespace
}  // End peloton namespace
