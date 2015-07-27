/*-------------------------------------------------------------------------
 *
 * index.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/index/index.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>
#include <string>

#include "backend/catalog/schema.h"
#include "backend/common/types.h"
#include "backend/storage/tuple.h"

namespace peloton {
namespace index {

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

  ~IndexMetadata() {
    // clean up key schema
    delete key_schema;
    // no need to clean the tuple schema
  }

  const std::string &GetName() const { return index_name; }

  oid_t GetOid() { return index_oid; }

  IndexType GetIndexMethodType() { return method_type; }

  IndexConstraintType GetIndexType() { return index_type; }

  const catalog::Schema *GetKeySchema() const { return key_schema; }

  int GetColumnCount() const { return GetKeySchema()->GetColumnCount(); }

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
class Index {
  friend class IndexFactory;

 public:
  oid_t GetOid() const { return index_oid; }

  IndexMetadata *GetMetadata() const { return metadata; }

  virtual ~Index() {
    // clean up metadata
    delete metadata;
  }

  //===--------------------------------------------------------------------===//
  // Mutators
  //===--------------------------------------------------------------------===//

  // insert an index entry linked to given tuple
  virtual bool InsertEntry(const storage::Tuple *key, ItemPointer location) = 0;

  // delete the index entry linked to given tuple
  virtual bool DeleteEntry(const storage::Tuple *key) = 0;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // return whether the entry is already stored in the index
  virtual bool Exists(const storage::Tuple *key) const = 0;

  // scan all keys in the index
  virtual std::vector<ItemPointer> Scan() const = 0;

  // get the locations of tuples matching given key
  virtual std::vector<ItemPointer> GetLocationsForKey(
      const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is between given start and end keys
  virtual std::vector<ItemPointer> GetLocationsForKeyBetween(
      const storage::Tuple *start, const storage::Tuple *end) const = 0;

  // get the locations of tuples whose key is less than given key
  virtual std::vector<ItemPointer> GetLocationsForKeyLT(
      const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is less than or equal to given key
  virtual std::vector<ItemPointer> GetLocationsForKeyLTE(
      const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is greater than given key
  virtual std::vector<ItemPointer> GetLocationsForKeyGT(
      const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is greater than or equal to given key
  virtual std::vector<ItemPointer> GetLocationsForKeyGTE(
      const storage::Tuple *key) const = 0;

  //===--------------------------------------------------------------------===//
  // STATS
  //===--------------------------------------------------------------------===//

  void IncreaseNumberOfTuplesBy(const float amount);

  void DecreaseNumberOfTuplesBy(const float amount);

  void SetNumberOfTuples(const float num_tuples);

  float GetNumberOfTuples() const;

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

  int GetColumnCount() const { return metadata->GetColumnCount(); }

  const std::string &GetName() const { return metadata->GetName(); }

  const catalog::Schema *GetKeySchema() const {
    return metadata->GetKeySchema();
  }

  IndexType GetIndexMethodType() { return metadata->GetIndexMethodType(); }

  IndexConstraintType GetIndexType() const { return metadata->GetIndexType(); }

  void GetInfo() const;

  // Get a string representation of this index
  friend std::ostream &operator<<(std::ostream &os, const Index &index);

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
  float number_of_tuples = 0.0;
};

}  // End index namespace
}  // End peloton namespace
