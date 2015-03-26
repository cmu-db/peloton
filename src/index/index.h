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

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/types.h"
#include "storage/tuple.h"

namespace nstore {
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
                IndexType type,
                catalog::Catalog *catalog,
                catalog::Schema *tuple_schema,
                const std::vector<id_t>& table_columns_in_key,
                bool unique,
                bool ints_only)

 : identifier(identifier),
   type(type),
   catalog(catalog),
   tuple_schema(tuple_schema),
   key_schema(nullptr),
   table_columns_in_key(table_columns_in_key),
   unique(unique),
   ints_only(ints_only) {

    key_schema = catalog::Schema::CopySchema(tuple_schema, table_columns_in_key);

  }

  ~IndexMetadata() {
    // free up key schema
    delete key_schema;
  }

  void SetIndexType(IndexType _type) {
    type = _type;
  }

  std::string identifier;

  IndexType type;

  // catalog
  catalog::Catalog *catalog;

  // schema of tuple values
  catalog::Schema *tuple_schema;

  // schema of keys
  catalog::Schema *key_schema;

  // table columns in key schema
  // if column "i" of table schema is present in key schema
  // then the value "i" will be present in this vector
  std::vector<id_t> table_columns_in_key;

  // unique keys ?
  bool unique;

  // storing ints only ?
  bool ints_only;

};

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//

/**
 * Index class represents a secondary index on a table
 * mapping from key value to tuple pointers.
 *
 * Index is an abstract class without any
 * implementation. Implementation class is specialized to uniqueness,
 * column types and numbers for higher performance.
 *
 * @see IndexFactory
 */
class Index
{
  friend class IndexFactory;

 public:

  /**
   * adds passed value as an index entry linked to given tuple
   */
  virtual bool AddEntry(const ItemPointer *tuple) = 0;

  /**
   * removes the index entry linked to given value (and tuple
   * pointer, if it's non-unique index).
   */
  virtual bool DeleteEntry(const ItemPointer *tuple) = 0;

  /**
   * removes the index entry linked to old value and re-link it to new value.
   * The address of the newTupleValue is used as the value in the index (and multimaps) as
   * well as the key for the new entry.
   */
  virtual bool UpdateEntry(const ItemPointer *old_tuple,
                           const ItemPointer *new_tuple) = 0;


  virtual bool SetValue(const ItemPointer *tuple, const void* address) = 0;


  /**
   * just returns whether the value is already stored. no
   * modification occurs.
   */
  virtual bool Exists(const ItemPointer* values) = 0;

  /**
   * This method moves to the first tuple equal to given key.  To
   * iterate through all entries with the key (if non-unique index)
   * or all entries that follow the entry, use nextValueAtKey() and
   * advanceToNextKey().
   *
   * This method can be used <b>only for perfect matching</b> in
   * which the whole search key matches with at least one entry in
   * this index.  For example,
   * (a,b,c)=(1,3,2),(1,3,3),(2,1,2),(2,1,3)....
   *
   * This method works for "WHERE a=2 AND b=1 AND c>=2", but does
   * not work for "WHERE a=2 AND b=1 AND c>=1". For partial index
   * search, use moveToKeyOrGreater.
   *
   * @see searchKey the value to be searched. this is NOT tuple
   * data, but chosen values for this index. So, searchKey has to
   * contain values in this index's entry order.
   *
   * @see moveToKeyOrGreater(const ItemPointer *)
   * @return true if the value is found. false if not.
   */
  virtual bool MoveToKey(const storage::Tuple *search_key) = 0;

  /**
   * Find location of the specified tuple in the index
   */
  virtual bool MoveToTuple(const storage::Tuple *search_tuple) = 0;

  /**
   * sets the tuple to point the entry found by moveToKey().  call
   * this repeatedly to get all entries with the search key (for
   * non-unique index).
   *
   * @return nullptr if next value does not exist.
   */
  virtual storage::Tuple *NextValueAtKey() = 0;

  /**
   * @return true if lhs is different from rhs in this index, which
   * means updateEntry has to follow.
   */
  virtual bool CheckForIndexChange(const ItemPointer *lhs,
                                   const ItemPointer *rhs) = 0;

  //===--------------------------------------------------------------------===//
  // Advanced methods
  //===--------------------------------------------------------------------===//

  /**
   * sets the tuple to point the entry next to the one found by
   * moveToKey().  calls this repeatedly to get all entries
   * following to the search key (for range query).
   *
   * HOWEVER, this can't be used for partial index search. You can
   * use this only when you in advance know that there is at least
   * one entry that perfectly matches with the search key. In other
   * word, this method SHOULD NOT BE USED in future because there
   * isn't such a case for range query except for cheating case
   * (i.e. TPCC slev which assumes there is always "OID-20" entry).
   *
   * @return true if any entry to return, false if not.
   */
  virtual bool AdvanceToNextKey() {
    throw NotImplementedException("Invoked Index virtual method AdvanceToNextKey which has no implementation");
  };

  /**
   * This method moves to the first tuple equal or greater than
   * given key.  Use this with nextValue(). This method works for
   * partial index search where following value might not match with
   * any entry in this index.
   *
   * @see searchKey the value to be searched. this is NOT tuple
   *      data, but chosen values for this index.  So, searchKey has
   *      to contain values in this index's entry order.
   */
  virtual void MoveToKeyOrGreater(__attribute__((unused)) const ItemPointer *search_key) {
    throw NotImplementedException("Invoked Index virtual method MoveToKeyOrGreater which has no implementation");
  };

  /**
   * This method moves to the first tuple greater than given key.
   * Use this with nextValue().
   *
   * @see searchKey the value to be searched. this is NOT tuple
   *      data, but chosen values for this index.  So, searchKey has
   *      to contain values in this index's entry order.
   */
  virtual void MoveToGreaterThanKey(__attribute__((unused)) const ItemPointer *search_key) {
    throw NotImplementedException("Invoked Index virtual method MoveToGreaterThanKey which has no implementation");
  };

  /**
   * This method moves to the beginning or the end of the indexes.
   * Use this with nextValue().
   *
   * @see begin true to move to the beginning, false to the end.
   */
  virtual void MoveToEnd(__attribute__((unused)) bool begin) {
    throw NotImplementedException("Invoked Index virtual method MoveToEnd which has no implementation");
  }

  /**
   * sets the tuple to point the entry found by
   * moveToKeyOrGreater().  calls this repeatedly to get all entries
   * with or following to the search key.
   *
   * @return true if any entry to return, false if reached the end
   * of this index.
   */
  virtual storage::Tuple *NextValue() {
    throw NotImplementedException("Invoked Index virtual method NextValue which has no implementation");
  };

  //===--------------------------------------------------------------------===//
  // Getters
  //===--------------------------------------------------------------------===//

  /**
   * Currently, UniqueIndex is just a Index with additional checks.
   * We might have to make a different class in future for maximizing
   * performance of UniqueIndex.
   */
  inline bool IsUniqueIndex() const {
    return is_unique_index;
  }

  virtual size_t GetSize() const = 0;

  // Return the amount of memory we think is allocated for this
  // index.
  virtual int64_t GetMemoryEstimate() const = 0;

  int GetColumnCount() const {
    return column_count;
  }

  const std::string& GetName() const {
    return identifier;
  }

  const catalog::Schema * GetKeySchema() const {
    return key_schema;
  }

  // Get a string representation of this index
  friend std::ostream& operator<<(std::ostream& os, const Index& index);

  virtual std::string GetTypeName() const = 0;

  virtual void EnsureCapacity(__attribute__((unused)) size_t capacity) {
  }

  // Print out info about lookup usage
  virtual void PrintReport();

  IndexMetadata GetScheme() const {
    return metadata;
  }

 protected:

  Index(const IndexMetadata &scheme);

  //===--------------------------------------------------------------------===//
  //  Data members
  //===--------------------------------------------------------------------===//

  const IndexMetadata metadata;

  std::string identifier;

  catalog::Catalog *catalog;

  catalog::Schema* key_schema;

  catalog::Schema *tuple_schema;

  int column_count;

  bool is_unique_index;

  // access counters
  int lookup_counter;
  int insert_counter;
  int delete_counter;
  int update_counter;

  // stats ?

  // index memory size
  int64_t memory_size_estimate;
};

} // End index namespace
} // End nstore namespace

