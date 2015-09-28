//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// index_key.h
//
// Identification: src/backend/index/index_key.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value_peeker.h"
#include "backend/common/logger.h"
#include "backend/storage/tuple.h"
#include "backend/index/index.h"

#include <cassert>
#include <iostream>
#include <sstream>

namespace peloton {
namespace index {

/*
 * Convert from a uint64_t that has had a signed number packed into it to
 * the specified signed type. The max signed value for that type is supplied as
 * the TypeMaxValue template parameter.
 */
template <typename SignedType, int64_t TypeMaxValue>
inline static SignedType ConvertUnsignedValueToSignedValue(uint64_t value) {
  int64_t retval = static_cast<int64_t>(value);
  retval -= TypeMaxValue + 1;
  return static_cast<SignedType>(retval);
}

/*
 * Specialization of convertUnsignedValueToSignedValue for int64_t. int64_t
 * requires a comparison
 * to prevent overflow.
 */
template <>
inline int64_t ConvertUnsignedValueToSignedValue<int64_t, INT64_MAX>(
    uint64_t value) {
  if (value > static_cast<uint64_t>(INT64_MAX) + 1) {
    value -= INT64_MAX;
    value--;
    return static_cast<int64_t>(value);
  } else {
    int64_t retval = static_cast<int64_t>(value);
    retval -= INT64_MAX;
    retval--;
    return retval;
  }
}

/*
 * Convert from a signed value to an unsigned value. The max value for the type
 * is supplied as a template
 * parameter. int64_t is used for all types to prevent overflow.
 */
template <int64_t TypeMaxValue, typename SignedValueType,
          typename UnsignedValueType>
inline static UnsignedValueType ConvertSignedValueToUnsignedValue(
    SignedValueType value) {
  return static_cast<UnsignedValueType>(value + TypeMaxValue + 1);
}

/*
 * Specialization for int64_ts necessary to prevent overflow.
 */
template <>
inline uint64_t ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
    int64_t value) {
  uint64_t retval = 0;
  if (value < 0) {
    value += INT64_MAX;
    value++;
    retval = static_cast<uint64_t>(value);
  } else {
    retval = static_cast<uint64_t>(value);
    retval += INT64_MAX;
    retval++;
  }
  return retval;
}

/**
 *  Integer key that will pack all key data into KeySize number of uint64_t.
 *  The minimum number of uint64_ts necessary to pack all the integers is used.
 */
template <std::size_t KeySize>
class IntsKey {
 public:
  /*
   * Take a value that is part of the key (already converted to a uint64_t) and
   *inserts it into the
   * most significant bytes available in the key. Templated on the size of the
   *type of key being inserted.
   * This allows the compiler to unroll the loop.
   *
   *
   * Algorithm is:
   * Start with the most significant byte of the key_value we are inserting and
   *put into the most significant byte
   * of the uint64_t portion of the key that is indexed by key_offset. The most
   *significant available byte within
   * the uint64_t portion of the key is indexed by intra_key_offset. Both
   *key_offset and intra_key_offset are passed
   * by reference so they can be updated.
   *
   *
   * Rinse and repeat with the less significant bytes.
   *
   */
  template <typename KeyValueType>
  inline void InsertKeyValue(int &key_offset, int &intra_key_offset,
                             uint64_t key_value) {
    for (int ii = static_cast<int>(sizeof(KeyValueType)) - 1; ii >= 0; ii--) {
      /*
       * Extract the most significant byte from key_value by shifting it all the
       * way to the right.
       * Mask off the rest. Then shift it to the left to the most significant
       * byte location available
       * in the key and OR it in.
       */
      data[key_offset] |= (0xFF & (key_value >> (ii * 8)))
                          << (intra_key_offset * 8);  //
      intra_key_offset--;  // Move the offset inside the uint64 key back one.
                           /*
                            * If there are no more bytes available in the uint64_t indexed by
                            * key_offset then increment key_offset
                            * to point to the next uint64_t and set intra_key_offset to index to the
                            * most significant byte
                            * in this next uint64_t.
                            */
      if (intra_key_offset < 0) {
        intra_key_offset = sizeof(uint64_t) - 1;
        key_offset++;
      }
    }
  }

  /*
   * Inverse of insertKeyValue.
   */
  template <typename KeyValueType>
  inline uint64_t ExtractKeyValue(int &key_offset,
                                  int &intra_key_offset) const {
    uint64_t retval = 0;
    for (int ii = static_cast<int>(sizeof(KeyValueType)) - 1; ii >= 0; ii--) {
      retval |= (0xFF & (data[key_offset] >> (intra_key_offset * 8)))
                << (ii * 8);
      intra_key_offset--;
      if (intra_key_offset < 0) {
        intra_key_offset = sizeof(uint64_t) - 1;
        key_offset++;
      }
    }
    return retval;
  }

  const storage::Tuple GetTupleForComparison(
		  __attribute__((unused)) const catalog::Schema *key_schema) {
    throw IndexException("Tuple conversion not supported");
  }

  std::string Debug(const catalog::Schema *key_schema) const {
    std::ostringstream buffer;
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    const int GetColumnCount = key_schema->GetColumnCount();
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint64_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int64_t, INT64_MAX>(
                        key_value) << ",";
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const uint64_t key_value =
              ExtractKeyValue<uint32_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int32_t, INT32_MAX>(
                        key_value) << ",";
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint16_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int16_t, INT16_MAX>(
                        key_value) << ",";
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint8_t>(key_offset, intra_key_offset);
          buffer << static_cast<int64_t>(
                        ConvertUnsignedValueToSignedValue<int8_t, INT8_MAX>(
                            key_value)) << ",";
          break;
        }
        default:
          throw IndexException(
              "We currently only support a specific set of "
              "column index sizes...");
          break;
      }
    }
    return std::string(buffer.str());
  }

  inline void SetFromKey(const storage::Tuple *tuple) {
    ::memset(data, 0, KeySize * sizeof(uint64_t));
    assert(tuple);
    const catalog::Schema *key_schema = tuple->GetSchema();
    const int GetColumnCount = key_schema->GetColumnCount();
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const int64_t value = ValuePeeker::PeekBigInt(tuple->GetValue(ii));
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const int32_t value = ValuePeeker::PeekInteger(tuple->GetValue(ii));
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const int16_t value = ValuePeeker::PeekSmallInt(tuple->GetValue(ii));
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const int8_t value = ValuePeeker::PeekTinyInt(tuple->GetValue(ii));
          const uint8_t key_value =
              ConvertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(
                  value);
          InsertKeyValue<uint8_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        default:
          throw IndexException(
              "We currently only support a specific set of "
              "column index sizes...");
          break;
      }
    }
  }

  inline void SetFromTuple(const storage::Tuple *tuple, const int *indices,
                           const catalog::Schema *key_schema) {
    ::memset(data, 0, KeySize * sizeof(uint64_t));
    const int GetColumnCount = key_schema->GetColumnCount();
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const int64_t value =
              ValuePeeker::PeekBigInt(tuple->GetValue(indices[ii]));
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const int32_t value =
              ValuePeeker::PeekInteger(tuple->GetValue(indices[ii]));
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const int16_t value =
              ValuePeeker::PeekSmallInt(tuple->GetValue(indices[ii]));
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const int8_t value =
              ValuePeeker::PeekTinyInt(tuple->GetValue(indices[ii]));
          const uint8_t key_value =
              ConvertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(
                  value);
          InsertKeyValue<uint8_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        default:
          throw IndexException(
              "We currently only support a specific set of "
              "column index sizes...");
          break;
      }
    }
  }

  // actual location of data
  uint64_t data[KeySize];

 private:
};

/** comparator for Int specialized indexes. */
template <std::size_t KeySize>
class IntsComparator {
 public:
  IntsComparator(index::IndexMetadata *metadata)
      : key_schema(metadata->GetKeySchema()) {}

  inline bool operator()(const IntsKey<KeySize> &lhs,
                         const IntsKey<KeySize> &rhs) const {
    // lexicographical compare could be faster for fixed N
    /*
     * Hopefully the compiler can unroll this loop
     */
    for (unsigned int ii = 0; ii < KeySize; ii++) {
      const uint64_t *lvalue = &lhs.data[ii];
      const uint64_t *rvalue = &rhs.data[ii];
      if (*lvalue < *rvalue) {
        return true;
      } else if (*lvalue > *rvalue) {
        return false;
      }
    }
    return false;
  }

  const catalog::Schema *key_schema;
};

/**
 *
 */
template <std::size_t KeySize>
class IntsEqualityChecker {
 public:
  IntsEqualityChecker(index::IndexMetadata *metadata)
      : key_schema(metadata->GetKeySchema()) {}

  inline bool operator()(const IntsKey<KeySize> &lhs,
                         const IntsKey<KeySize> &rhs) const {
    for (unsigned int ii = 0; ii < KeySize; ii++) {
      const uint64_t *lvalue = &lhs.data[ii];
      const uint64_t *rvalue = &rhs.data[ii];

      if (*lvalue != *rvalue) {
        return false;
      }
    }
    return true;
  }

  const catalog::Schema *key_schema;
};

/**
 *
 */
template <std::size_t KeySize>
struct IntsHasher : std::unary_function<IntsKey<KeySize>, std::size_t> {
  IntsHasher(catalog::Schema *key_schema) {}

  inline size_t operator()(IntsKey<KeySize> const &p) const {
    size_t seed = 0;
    for (int ii = 0; ii < KeySize; ii++) {
      boost::hash_combine(seed, p.data[ii]);
    }
    return seed;
  }
};

/**
 * Key object for indexes of mixed types.
 * Using storage::Tuple to store columns.
 */
template <std::size_t KeySize>
class GenericKey {
 public:
  inline void SetFromKey(const storage::Tuple *tuple) {
    assert(tuple);
    ::memcpy(data, tuple->GetData(), KeySize);
  }

  inline void SetFromTuple(const storage::Tuple *tuple, const int *indices,
                           const catalog::Schema *key_schema) {
    storage::Tuple key_tuple(key_schema);
    key_tuple.MoveToTuple(reinterpret_cast<void *>(data));
    for (int col_itr = 0; col_itr < key_schema->GetColumnCount(); col_itr++) {
      key_tuple.SetValue(col_itr, tuple->GetValue(indices[col_itr]));
    }
  }

  const storage::Tuple GetTupleForComparison(
      const catalog::Schema *key_schema) {
    return storage::Tuple(key_schema, data);
  }

  // actual location of data, extends past the end.
  char data[KeySize];

 private:
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparator {
 public:
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericComparator(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    storage::Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    storage::Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    // lexicographical compare could be faster for fixed N
    int diff = lhTuple.Compare(rhTuple);
    return diff < 0;
  }

  const catalog::Schema *schema;
};

/**
 * Equality-checking function object
 */
template <std::size_t KeySize>
class GenericEqualityChecker {
 public:
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericEqualityChecker(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    storage::Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    storage::Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  const catalog::Schema *schema;
};

/**
 * Hash function object for an array of SlimValues
 */
template <std::size_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericHasher(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const {
    storage::Tuple pTuple(schema);
    pTuple.MoveToTuple(reinterpret_cast<const void *>(&p));
    return pTuple.HashCode();
  }

  const catalog::Schema *schema;
};

/*
 * TupleKey is the all-purpose fallback key for indexes that can't be
 * better specialized. Each TupleKey wraps a pointer to a *persistent
 * table tuple*. TableIndex knows the column indices from the
 * persistent table that form the index key. TupleKey uses this data
 * to evaluate and compare keys by extracting and comparing
 * the appropriate columns' values.
 *
 * Note that the index code will create keys in the schema of the
 * the index key. While all TupleKeys resident in the index itself
 * will point to persistent tuples, there are ephemeral TupleKey
 * instances that point to tuples in the index key schema.
 *
 * Pros: supports any combination of columns in a key. Each index
 * key is 24 bytes (a pointer to a tuple and a pointer to the column
 * indices (which map index columns to table columns).
 *
 * Cons: requires an indirection to evaluate a key (must follow the
 * the pointer to read the underlying storage::Tuple). Compares what are
 * probably very wide keys one column at a time by initializing and
 * comparing Values.
 */
class TupleKey {
 public:
  inline TupleKey() {
    column_indices = NULL;
    key_tuple = NULL;
    key_tuple_schema = NULL;
  }

  // Set a key from a key-schema tuple.
  inline void SetFromKey(const storage::Tuple *tuple) {
    assert(tuple);
    column_indices = NULL;
    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
  }

  // Set a key from a table-schema tuple.
  inline void SetFromTuple(const storage::Tuple *tuple, const int *indices,
		  	  __attribute__((unused)) const catalog::Schema *key_schema) {
    assert(tuple);
    assert(indices);
    column_indices = indices;
    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
  }

  // Return true if the TupleKey references an ephemeral index key.
  bool IsKeySchema() const { return column_indices == NULL; }

  // Return a table tuple that is valid for comparison
  const storage::Tuple GetTupleForComparison(
      const catalog::Schema *key_tuple_schema) const {
    return storage::Tuple(key_tuple_schema, key_tuple);
  }

  // Return the indexColumn'th key-schema column.
  int ColumnForIndexColumn(int indexColumn) const {
    if (IsKeySchema())
      return indexColumn;
    else
      return column_indices[indexColumn];
  }

  // TableIndex owns this array - NULL if an ephemeral key
  const int *column_indices;

  // Pointer a persistent tuple in non-ephemeral case.
  char *key_tuple;
  const catalog::Schema *key_tuple_schema;
};

class TupleKeyComparator {
 public:
  TupleKeyComparator(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  // return true if lhs < rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    Value lhValue, rhValue;

    for (unsigned int col_itr = 0; col_itr < schema->GetColumnCount(); ++col_itr) {
      lhValue = lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr));
      rhValue = rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr));
      int comparison = lhValue.Compare(rhValue);

      if (comparison == VALUE_COMPARE_LESSTHAN) {
        return true;
      } else if (comparison == VALUE_COMPARE_GREATERTHAN) {
        return false;
      }
    }
    return false;
  }

  const catalog::Schema *schema;
};

class TupleKeyEqualityChecker {
 public:
  TupleKeyEqualityChecker(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  // return true if lhs == rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    Value lhValue, rhValue;

    for (unsigned int col_itr = 0; col_itr < schema->GetColumnCount(); ++col_itr) {
      lhValue = lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr));
      rhValue = rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr));

      if (lhValue.Compare(rhValue) != VALUE_COMPARE_EQUAL) {
        return false;
      }
    }
    return true;
  }

  const catalog::Schema *schema;
};

}  // End index namespace
}  // End peloton namespace
