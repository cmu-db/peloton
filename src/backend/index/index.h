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

  IndexMetadata(std::string identifier,
                IndexMethodType type,
                const catalog::Schema *tuple_schema,
                const catalog::Schema *key_schema,
                bool unique_keys)

 : identifier(identifier),
   method_type(type),
   tuple_schema(tuple_schema),
   key_schema(key_schema),
   unique_keys(unique_keys) {

  }

  ~IndexMetadata(){
    // clean up key schema
    delete key_schema;

    // no need to clean the tuple schema
  }

  void SetIndexMethodType(IndexMethodType _type) {
    method_type = _type;
  }
  void SetIndexType(IndexType _type) {
    type = _type;
  }

  std::string identifier;

  IndexMethodType method_type;

  IndexType type;

  // schema of tuple values
  const catalog::Schema *tuple_schema;

  // schema of keys
  const catalog::Schema *key_schema;

  // table columns in key schema
  // if column "i" of table schema is present in key schema
  // then the value "i" will be present in this vector
  std::vector<oid_t> table_columns_in_key;

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
class Index
{
  friend class IndexFactory;

 public:

  virtual ~Index(){

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
  virtual std::vector<ItemPointer> GetLocationsForKey(const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is between given start and end keys
  virtual std::vector<ItemPointer> GetLocationsForKeyBetween(const storage::Tuple *start, const storage::Tuple *end) const = 0;

  // get the locations of tuples whose key is less than given key
  virtual std::vector<ItemPointer> GetLocationsForKeyLT(const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is less than or equal to given key
  virtual std::vector<ItemPointer> GetLocationsForKeyLTE(const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is greater than given key
  virtual std::vector<ItemPointer> GetLocationsForKeyGT(const storage::Tuple *key) const = 0;

  // get the locations of tuples whose key is greater than or equal to given key
  virtual std::vector<ItemPointer> GetLocationsForKeyGTE(const storage::Tuple *key)  const = 0;

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  virtual std::string GetTypeName() const = 0;

  /**
   * Currently, UniqueIndex is just an index with additional checks.
   * We might have to make a different class in future for maximizing
   * performance of UniqueIndex.
   */
  bool HasUniqueKeys() const {
    return unique_keys;
  }


  int GetColumnCount() const {
    return column_count;
  }

  const std::string& GetName() const {
    return identifier;
  }

  const catalog::Schema *GetKeySchema() const {
    return key_schema;
  }

  // Get a string representation of this index
  friend std::ostream& operator<<(std::ostream& os, const Index& index);

  void GetInfo() const;

  IndexMetadata *GetMetadata() const {
    return metadata;
  }

 protected:

  Index(IndexMetadata *schema);

  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//

  IndexMetadata *metadata;

  std::string identifier;

  const catalog::Schema *key_schema;

  const catalog::Schema *tuple_schema;

  int column_count;

  bool unique_keys;

  // access counters
  int lookup_counter;
  int insert_counter;
  int delete_counter;
  int update_counter;

};

} // End index namespace
} // End peloton namespace

