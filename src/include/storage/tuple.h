//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple.h
//
// Identification: src/include/storage/tuple.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "catalog/schema.h"
#include "common/abstract_tuple.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "common/internal_types.h"
#include "type/value.h"

namespace peloton {
namespace storage {

//===--------------------------------------------------------------------===//
// Tuple class
//===--------------------------------------------------------------------===//

class Tuple : public AbstractTuple {
  friend class catalog::Schema;
  friend class ValuePeeker;
  friend class Tile;

 public:
  // Default constructor (don't use this)
  inline Tuple()
      : tuple_schema_(nullptr), tuple_data_(nullptr), allocated_(false) {}

  // Setup the tuple given a tuple
  inline Tuple(const Tuple &rhs)
      : tuple_schema_(rhs.tuple_schema_),
        tuple_data_(rhs.tuple_data_),
        allocated_(false) {}

  // Setup the tuple given a schema
  inline Tuple(const catalog::Schema *schema)
      : tuple_schema_(schema), tuple_data_(nullptr), allocated_(false) {
    PL_ASSERT(tuple_schema_);
  }

  // Setup the tuple given a schema and location
  inline Tuple(const catalog::Schema *schema, char *data)
      : tuple_schema_(schema), tuple_data_(data), allocated_(false) {
    PL_ASSERT(tuple_schema_);
    PL_ASSERT(tuple_data_);
  }

  // Setup the tuple given a schema and allocate space
  inline Tuple(const catalog::Schema *schema, bool allocate)
      : tuple_schema_(schema), tuple_data_(nullptr), allocated_(allocate) {
    PL_ASSERT(tuple_schema_);

    if (allocated_) {
      // initialize heap allocation
      tuple_data_ = new char[tuple_schema_->GetLength()]();
    }
  }

  // Deletes tuple data
  // Does not delete either SCHEMA
  ~Tuple();

  // Setup the tuple given the specified data location and schema
  Tuple(char *data, catalog::Schema *schema);

  // Assignment operator
  Tuple &operator=(const Tuple &rhs);

  void Copy(const void *source, type::AbstractPool *pool = NULL);

  /**
   * Set the tuple to point toward a given address in a table's
   * backing store
   */
  inline void Move(void *address) {
    tuple_data_ = reinterpret_cast<char *>(address);
  }

  bool operator==(const Tuple &other) const;
  bool operator!=(const Tuple &other) const;

  int Compare(const Tuple &other) const;

  int Compare(const Tuple &other, const std::vector<oid_t> &columns) const;

  //===--------------------------------------------------------------------===//
  // Getters and Setters
  //===--------------------------------------------------------------------===//

  // This is used to access the internal array to read simple data types
  // such as integer type
  template <typename ColumnType>
  inline ColumnType GetInlinedDataOfType(oid_t column_id) const;

  // Get the value of a specified column (const)
  // (expensive) checks the schema to see how to return the Value.
  type::Value GetValue(oid_t column_id) const;

  /**
   * Allocate space to copy strings that can't be inlined rather
   * than copying the pointer.
   * It is also possible to provide NULL for stringPool in which case
   * the strings will be allocated on the heap.
   */
  void SetValue(const oid_t column_id, const type::Value &value,
                type::AbstractPool *dataPool);

  // set value without data pool.
  // This just calls the other SetValue with a nullptr pool
  void SetValue(oid_t column_id, const type::Value &value) {
    SetValue(column_id, value, nullptr);
  }

  inline int GetLength() const { return tuple_schema_->GetLength(); }

  // Is the column value null ?
  inline bool IsNull(const uint64_t column_id) const {
    type::Value value = (GetValue(column_id));
    return value.IsNull();
  }

  // Is the tuple null ?
  inline bool IsNull() const { return tuple_data_ == NULL; }

  // Get the type of a particular column in the tuple
  inline type::TypeId GetType(int column_id) const {
    return tuple_schema_->GetType(column_id);
  }

  inline const catalog::Schema *GetSchema() const { return tuple_schema_; }

  // Get the address of this tuple in the table's backing store
  inline char *GetData() const { return tuple_data_; }

  char *GetDataPtr(const oid_t column_id);

  const char *GetDataPtr(const oid_t column_id) const;

  // Return the number of columns in this tuple
  inline oid_t GetColumnCount() const {
    return tuple_schema_->GetColumnCount();
  }

  bool EqualsNoSchemaCheck(const AbstractTuple &other) const;

  bool EqualsNoSchemaCheck(const AbstractTuple &other,
                           const std::vector<oid_t> &columns) const;

  // this does set NULL in addition to clear string count.
  void SetAllNulls();

  void SetNull() { tuple_data_ = NULL; }

  // this does set 0 to all values. VarlenValue is set to "0"
  void SetAllZeros();

  /**
   * Determine the maximum number of bytes when serialized for Export.
   * Excludes the bytes required by the row header (which includes
   * the null bit indicators) and ignores the width of metadata columns.
   */
  size_t ExportSerializationSize() const;

  // Return the amount of memory allocated for non-inlined objects
  size_t GetUninlinedMemorySize() const;

  // This sets the relevant columns from the source tuple
  void SetFromTuple(const AbstractTuple *tuple,
                    const std::vector<oid_t> &columns,
                    type::AbstractPool *pool);

  // Used to wrap read only tuples in indexing code.
  void MoveToTuple(const void *address);

  //===--------------------------------------------------------------------===//
  // Serialization utilities
  //===--------------------------------------------------------------------===//

  void SerializeTo(SerializeOutput &output);
  void SerializeToExport(SerializeOutput &output, int col_offset,
                         uint8_t *null_array);
  void SerializeWithHeaderTo(SerializeOutput &output);

  void DeserializeFrom(SerializeInput &input, type::AbstractPool *pool);
  void DeserializeWithHeaderFrom(SerializeInput &input);

  size_t HashCode(size_t seed) const;
  size_t HashCode() const;

  // Get a string representation for debugging
  const std::string GetInfo() const;

 private:
  //===--------------------------------------------------------------------===//
  // Data members
  //===--------------------------------------------------------------------===//

  // The types of the columns in the tuple
  const catalog::Schema *tuple_schema_;

  // The tuple data, padded at the front by the TUPLE_HEADER
  char *tuple_data_;

  // Allocated or not ?
  bool allocated_;
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

/*
 * GetInlineDataOfType() - This functions returns a reinterpreted object of
 *                         a type given by the caller
 *
 * Please note this function simply translates column ID into offsets into
 * the data array. Non-inlined objects and objects that need special treatment
 * could not be copied like this, and they must use a Value object
 *
 * NOTE: It assumes all fileds are inlined. This should be checked elsewhere
 */
template <typename ColumnType>
inline ColumnType Tuple::GetInlinedDataOfType(oid_t column_id) const {
  // The requested field must be inlined
  PL_ASSERT(tuple_schema_->IsInlined(column_id) == true);
  PL_ASSERT(column_id < GetColumnCount());

  // Translates column ID into a pointer and converts it to the
  // requested type
  const ColumnType *ptr =
      reinterpret_cast<const ColumnType *>(GetDataPtr(column_id));

  return *ptr;
}

// Setup the tuple given the specified data location and schema
inline Tuple::Tuple(char *data, catalog::Schema *schema) {
  PL_ASSERT(data);
  PL_ASSERT(schema);

  tuple_data_ = data;
  tuple_schema_ = schema;
  allocated_ = false;  // ???
}

inline Tuple &Tuple::operator=(const Tuple &rhs) {
  tuple_schema_ = rhs.tuple_schema_;
  tuple_data_ = rhs.tuple_data_;
  return *this;
}

//===--------------------------------------------------------------------===//
// Tuple Hasher
//===--------------------------------------------------------------------===//

struct TupleHasher : std::unary_function<Tuple, std::size_t> {
  // Generate a 64-bit number for the key value
  size_t operator()(Tuple tuple) const { return tuple.HashCode(); }
};

//===--------------------------------------------------------------------===//
// Tuple Comparator
//===--------------------------------------------------------------------===//

class TupleComparator {
 public:
  bool operator()(const Tuple lhs, const Tuple rhs) const {
    return lhs.EqualsNoSchemaCheck(rhs);
  }
};

}  // namespace storage
}  // namespace peloton
