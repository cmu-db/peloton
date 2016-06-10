//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index.h
//
// Identification: src/backend/index/index.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>
#include <string>
#include <functional>
#include <memory>

#include "backend/common/printable.h"
#include "backend/common/types.h"
#include "backend/common/macros.h"

namespace peloton {

class AbstractTuple;
class VarlenPool;

namespace catalog { class Schema; }

namespace storage { class Tuple; }

namespace index {

class RBItemPointer {
public:
  RBItemPointer(const ItemPointer &loc, const cid_t &ts):
    location(loc),
    timestamp(ts) {}
  ItemPointer location;
  cid_t timestamp;

  bool operator==(const RBItemPointer &other) {
    return this->location == other.location && this->timestamp == other.timestamp;
  }
};

//===--------------------------------------------------------------------===//
// IndexMetadata
//===--------------------------------------------------------------------===//

/**
 * Parameter for constructing Index. catalog::Schema, then key schema
 */
class IndexMetadata {
  IndexMetadata() = delete;

 public:
  IndexMetadata(std::string index_name, oid_t index_oid, IndexType method_type,
                IndexConstraintType index_type,
                const catalog::Schema *tuple_schema,
                const catalog::Schema *key_schema, bool unique_keys)
      : index_name(index_name),
        index_oid(index_oid),
        method_type(method_type),
        index_type(index_type),
        tuple_schema(tuple_schema),
        key_schema(key_schema),
        unique_keys(unique_keys) {}

  ~IndexMetadata();

  const std::string &GetName() const { return index_name; }

  oid_t GetOid() { return index_oid; }

  IndexType GetIndexMethodType() { return method_type; }

  IndexConstraintType GetIndexType() { return index_type; }

  const catalog::Schema *GetKeySchema() const { return key_schema; }

  oid_t GetColumnCount() const;

  bool HasUniqueKeys() const { return unique_keys; }

  std::string index_name;

  oid_t index_oid;

  IndexType method_type;

  IndexConstraintType index_type;

  // schema of tuple values
  const catalog::Schema *tuple_schema;

  // schema of keys
  const catalog::Schema *key_schema;

  // unique keys ?
  bool unique_keys;
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
  //
  // The forth argument is a return value of the inserted itempointer pointer,
  // which is nullptr if the insertion fail.
  virtual bool CondInsertEntry(
      const storage::Tuple *key, const ItemPointer &location,
      std::function<bool(const ItemPointer &)> predicate,
      ItemPointer **itempointer_ptr) = 0;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // scan all keys in the index matching an arbitrary key
  // used by index scan executor
  virtual void Scan(const std::vector<Value> &values,
                    const std::vector<oid_t> &key_column_ids,
                    const std::vector<ExpressionType> &exprs,
                    const ScanDirectionType &scan_direction,
                    std::vector<ItemPointer> &) = 0;

  // scan the entire index, working like a sort
  virtual void ScanAllKeys(std::vector<ItemPointer> &) = 0;

  virtual void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer> &) = 0;

  virtual void Scan(const std::vector<Value> &values,
                    const std::vector<oid_t> &key_column_ids,
                    const std::vector<ExpressionType> &exprs,
                    const ScanDirectionType &scan_direction,
                    std::vector<ItemPointer *> &result) = 0;

  virtual void ScanAllKeys(std::vector<ItemPointer *> &result) = 0;

  virtual void ScanKey(const storage::Tuple *key,
                       std::vector<ItemPointer *> &result) = 0;

  // For RB
  virtual bool InsertEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                           UNUSED_ATTRIBUTE const ItemPointer &location,
                           UNUSED_ATTRIBUTE RBItemPointer **result)
  {
    assert(false);
    return false;
  }

  virtual bool DeleteEntry(UNUSED_ATTRIBUTE const storage::Tuple *key,
                           UNUSED_ATTRIBUTE const RBItemPointer &rb_location)
  {
    assert(false);
    return false;
  }

  virtual bool CondInsertEntry(
      UNUSED_ATTRIBUTE const storage::Tuple *key, 
      UNUSED_ATTRIBUTE const ItemPointer &location,
      UNUSED_ATTRIBUTE std::function<bool(const ItemPointer &)> predicate,
      UNUSED_ATTRIBUTE RBItemPointer **rb_itempointer_ptr)
  {
    assert(false);
    return false;
  }

  virtual void Scan(UNUSED_ATTRIBUTE const std::vector<Value> &values,
                    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
                    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
                    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                    UNUSED_ATTRIBUTE std::vector<RBItemPointer> &)
  {
    assert(false);
  }

  // scan the entire index, working like a sort
  virtual void ScanAllKeys(UNUSED_ATTRIBUTE std::vector<RBItemPointer> &)
  {
    assert(false);
  }

  virtual void ScanKey(UNUSED_ATTRIBUTE const storage::Tuple *key,
                       UNUSED_ATTRIBUTE std::vector<RBItemPointer> &)
  {
    assert(false);
  }

  virtual void Scan(UNUSED_ATTRIBUTE const std::vector<Value> &values,
                    UNUSED_ATTRIBUTE const std::vector<oid_t> &key_column_ids,
                    UNUSED_ATTRIBUTE const std::vector<ExpressionType> &exprs,
                    UNUSED_ATTRIBUTE const ScanDirectionType &scan_direction,
                    UNUSED_ATTRIBUTE std::vector<RBItemPointer *> &result)
  {
    assert(false);
  }

  virtual void ScanAllKeys(UNUSED_ATTRIBUTE std::vector<RBItemPointer *> &result)
  {
    assert(false);
  }

  virtual void ScanKey(UNUSED_ATTRIBUTE const storage::Tuple *key,
                       UNUSED_ATTRIBUTE std::vector<RBItemPointer *> &result)
  {
    assert(false);
  }

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const float amount);

  void DecreaseNumberOfTuplesBy(const float amount);

  void SetNumberOfTuples(const float num_tuples);

  float GetNumberOfTuples() const;

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
  bool Compare(const AbstractTuple &index_key,
                      const std::vector<oid_t> &column_ids,
                      const std::vector<ExpressionType> &expr_types,
                      const std::vector<Value> &values);

  VarlenPool *GetPool() const { return pool; }

  // Garbage collect
  virtual bool Cleanup() = 0;

  // Get the memory footprint
  virtual size_t GetMemoryFootprint() = 0;

 protected:
  Index(IndexMetadata *schema);

  // Set the lower bound tuple for index iteration
  bool ConstructLowerBoundTuple(storage::Tuple *index_key,
                                const std::vector<Value> &values,
                                const std::vector<oid_t> &key_column_ids,
                                const std::vector<ExpressionType> &expr_types);

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
  float number_of_tuples = 0.0;

  // dirty flag
  bool dirty = false;

  // pool
  VarlenPool *pool = nullptr;
};

}  // End index namespace
}  // End peloton namespace
