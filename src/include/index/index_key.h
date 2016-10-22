//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// index_key.h
//
// Identification: src/include/index/index_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <sstream>

#include "common/value_peeker.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/tuple.h"
#include "index/index.h"

#include <boost/functional/hash.hpp>

namespace peloton {
namespace index {

using namespace peloton::common;

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
 *  class IntsKey - Integer key
 *
 * Integer key that will pack all key data into KeySize number of uint64_t.
 * The minimum number of uint64_ts necessary to pack all the integers is used.
 *
 * This will be hopefully optimized into direct integer manipulation whenever
 * possible by the compiler since the operations are just on an array of
 * integers
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
      UNUSED_ATTRIBUTE const catalog::Schema *key_schema) const {
    throw IndexException("Tuple conversion not supported");
  }

  std::string Debug(const catalog::Schema *key_schema) const {
    std::ostringstream buffer;
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    const int GetColumnCount = key_schema->GetColumnCount();
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case Type::BIGINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint64_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int64_t, INT64_MAX>(
                        key_value) << ",";
          break;
        }
        case Type::INTEGER: {
          const uint64_t key_value =
              ExtractKeyValue<uint32_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int32_t, INT32_MAX>(
                        key_value) << ",";
          break;
        }
        case Type::SMALLINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint16_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int16_t, INT16_MAX>(
                        key_value) << ",";
          break;
        }
        case Type::TINYINT: {
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
    PL_MEMSET(data, 0, KeySize * sizeof(uint64_t));
    PL_ASSERT(tuple);
    const catalog::Schema *key_schema = tuple->GetSchema();
    const int GetColumnCount = key_schema->GetColumnCount();
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case Type::BIGINT: {
          common::Value val = tuple->GetValue(ii);
          const int64_t value = ValuePeeker::PeekBigInt(val);
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::INTEGER: {
          common::Value val = tuple->GetValue(ii);
          const int32_t value = ValuePeeker::PeekInteger(val);
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::SMALLINT: {
          common::Value val = tuple->GetValue(ii);
          const int16_t value = ValuePeeker::PeekSmallInt(val);
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::TINYINT: {
          common::Value val = tuple->GetValue(ii);
          const int8_t value = ValuePeeker::PeekTinyInt(val);
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
    PL_MEMSET(data, 0, KeySize * sizeof(uint64_t));
    const int GetColumnCount = key_schema->GetColumnCount();
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case Type::BIGINT: {
          common::Value val = tuple->GetValue(indices[ii]);
          const int64_t value = ValuePeeker::PeekBigInt(val);
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::INTEGER: {
          common::Value val = tuple->GetValue(indices[ii]);
          const int32_t value =
              ValuePeeker::PeekInteger(val);
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::SMALLINT: {
          common::Value val = tuple->GetValue(indices[ii]);
          const int16_t value = ValuePeeker::PeekSmallInt(val);
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case Type::TINYINT: {
          common::Value val = tuple->GetValue(indices[ii]);
          const int8_t value = ValuePeeker::PeekTinyInt(val);
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

  IntsComparator(const IntsComparator &) {}
  IntsComparator() {}
};

/** comparator for Int specialized indexes. */
template <std::size_t KeySize>
class IntsComparatorRaw {
 public:
  inline int operator()(const IntsKey<KeySize> &lhs,
                        const IntsKey<KeySize> &rhs) const {
    // lexicographical compare could be faster for fixed N
    /*
     * Hopefully the compiler can unroll this loop
     */
    for (unsigned int ii = 0; ii < KeySize; ii++) {
      const uint64_t *lvalue = &lhs.data[ii];
      const uint64_t *rvalue = &rhs.data[ii];
      if (*lvalue < *rvalue) {
        return VALUE_COMPARE_LESSTHAN;
      } else if (*lvalue > *rvalue) {
        return VALUE_COMPARE_GREATERTHAN;
      }
    }

    /* equal */
    return VALUE_COMPARE_EQUAL;
  }

  IntsComparatorRaw(const IntsComparatorRaw &) {}
  IntsComparatorRaw() {}
};

/**
 *
 */
template <std::size_t KeySize>
class IntsEqualityChecker {
 public:
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

  IntsEqualityChecker(const IntsEqualityChecker &) {};
  IntsEqualityChecker() {};
};

/**
 *
 */
template <std::size_t KeySize>
struct IntsHasher : std::unary_function<IntsKey<KeySize>, std::size_t> {
  IntsHasher(index::IndexMetadata *metadata)
      : schema(metadata->GetKeySchema()) {}

  inline size_t operator()(IntsKey<KeySize> const &p) const {
    size_t seed = 0;
    for (size_t ii = 0; ii < KeySize; ii++) {
      boost::hash_combine(seed, p.data[ii]);
    }
    return seed;
  }

  const catalog::Schema *schema;

  IntsHasher(const IntsHasher &) {}
  IntsHasher() {};
};

/**
 * class GenericKey - Key used for indexing with opaque data
 *
 * This key type uses an fixed length array to hold data for indexing
 * purposes, the actual size of which is specified and instanciated
 * with a template argument.
 */
template <std::size_t KeySize>
class GenericKey {
 public:
  inline void SetFromKey(const storage::Tuple *tuple) {
    PL_ASSERT(tuple);
    PL_MEMCPY(data, tuple->GetData(), tuple->GetLength());
    schema = tuple->GetSchema();
  }

  const storage::Tuple GetTupleForComparison(
      const catalog::Schema *key_schema) {
    return storage::Tuple(key_schema, data);
  }

  inline const Value ToValueFast(const catalog::Schema *schema,
                                 int column_id) const {
    const Type::TypeId column_type = schema->GetType(column_id);
    const char *data_ptr = &data[schema->GetOffset(column_id)];
    const bool is_inlined = schema->IsInlined(column_id);

    return Value::DeserializeFrom(data_ptr, column_type, is_inlined);
  }

  // actual location of data, extends past the end.
  char data[KeySize];

  const catalog::Schema *schema;
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparator {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    for (oid_t column_itr = 0; column_itr < schema->GetColumnCount();
         column_itr++) {
      const common::Value lhs_value = (
          lhs.ToValueFast(schema, column_itr));
      const common::Value rhs_value = (
          rhs.ToValueFast(schema, column_itr));
      
      common::Value res_lt = (
          lhs_value.CompareLessThan(rhs_value));
      if (res_lt.IsTrue())
        return true;

      common::Value res_gt = (
          lhs_value.CompareGreaterThan(rhs_value));
      if (res_gt.IsTrue())
        return false;
    }

    return false;
  }

  GenericComparator(const GenericComparator &) {}
  GenericComparator() {}
};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t KeySize>
class GenericComparatorRaw {
 public:
  inline int operator()(const GenericKey<KeySize> &lhs,
                        const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    for (oid_t column_itr = 0; column_itr < schema->GetColumnCount();
         column_itr++) {
      const common::Value lhs_value = (
          lhs.ToValueFast(schema, column_itr));
      const common::Value rhs_value = (
          rhs.ToValueFast(schema, column_itr));

      common::Value res_lt = (
        lhs_value.CompareLessThan(rhs_value));
      if (res_lt.IsTrue())
        return VALUE_COMPARE_LESSTHAN;

      common::Value res_gt = (
        lhs_value.CompareGreaterThan(rhs_value));
      if (res_gt.IsTrue())
        return VALUE_COMPARE_GREATERTHAN;
    }

    /* equal */
    return VALUE_COMPARE_EQUAL;
  }

  GenericComparatorRaw(const GenericComparatorRaw &) {}
  GenericComparatorRaw() {}
};

/**
 * Equality-checking function object
 */
template <std::size_t KeySize>
class GenericEqualityChecker {
 public:
  inline bool operator()(const GenericKey<KeySize> &lhs,
                         const GenericKey<KeySize> &rhs) const {
    auto schema = lhs.schema;

    storage::Tuple lhTuple(schema);
    lhTuple.MoveToTuple(reinterpret_cast<const void *>(&lhs));
    storage::Tuple rhTuple(schema);
    rhTuple.MoveToTuple(reinterpret_cast<const void *>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  GenericEqualityChecker(const GenericEqualityChecker &) {}
  GenericEqualityChecker() {}
};

/**
 * Hash function object for an array of SlimValues
 */
template <std::size_t KeySize>
struct GenericHasher : std::unary_function<GenericKey<KeySize>, std::size_t> {

  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<KeySize> const &p) const {
    auto schema = p.schema;

    storage::Tuple pTuple(schema);
    pTuple.MoveToTuple(reinterpret_cast<const void *>(&p));
    return pTuple.HashCode();
  }

  GenericHasher(const GenericHasher &) {}
  GenericHasher() {};
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
    column_indices = nullptr;
    key_tuple = nullptr;
    key_tuple_schema = nullptr;
  }

  // Set a key from a key-schema tuple.
  inline void SetFromKey(const storage::Tuple *tuple) {
    PL_ASSERT(tuple);

    column_indices = nullptr;

    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
  }

  // Set a key from a table-schema tuple.
  inline void SetFromTuple(const storage::Tuple *tuple, const int *indices,
                           UNUSED_ATTRIBUTE const catalog::Schema *key_schema) {
    PL_ASSERT(tuple);
    PL_ASSERT(indices);
    column_indices = indices;
    key_tuple = tuple->GetData();
    key_tuple_schema = tuple->GetSchema();
  }

  // Return true if the TupleKey references an ephemeral index key.
  bool IsKeySchema() const { return column_indices == nullptr; }

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

/*
 * class TupleKeyHasher - Hash function for tuple keys
 */
struct TupleKeyHasher {

  /** Generate a 64-bit number for the key value */
  inline size_t operator()(const TupleKey &p) const {
    storage::Tuple pTuple = p.GetTupleForComparison(p.key_tuple_schema);
    return pTuple.HashCode();
  }

  /*
   * Copy Constructor - To make compiler happy
   */
  TupleKeyHasher(const TupleKeyHasher &) {}
  TupleKeyHasher() {};
};

class TupleKeyComparator {
 public:
  // return true if lhs < rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    auto schema = lhs.key_tuple_schema;

    for (unsigned int col_itr = 0; col_itr < schema->GetColumnCount();
         ++col_itr) {
      common::Value lhValue = (
          lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr)));
      common::Value rhValue = (
          rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr)));

      common::Value res_lt = (
          lhValue.CompareLessThan(rhValue));
      if (res_lt.IsTrue())
        return true;

      common::Value res_gt = (
          lhValue.CompareGreaterThan(rhValue));
      if (res_gt.IsTrue())
        return false;
    }
    return false;
  }

  /*
   * Copy constructor
   */
  TupleKeyComparator(const TupleKeyComparator &) {}
  TupleKeyComparator() {};
};

class TupleKeyComparatorRaw {
 public:
  // return -1 if a < b ;  0 if a == b ;  1 if a > b
  inline int operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    auto schema = lhs.key_tuple_schema;

    for (unsigned int col_itr = 0; col_itr < schema->GetColumnCount();
         ++col_itr) {
      common::Value lhValue = (
          lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr)));
      common::Value rhValue = (
          rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr)));

      common::Value res_lt = (
          lhValue.CompareLessThan(rhValue));
      if (res_lt.IsTrue())
        return VALUE_COMPARE_LESSTHAN;

      common::Value res_gt(
          lhValue.CompareGreaterThan(rhValue));
      if (res_gt.IsTrue())
        return VALUE_COMPARE_GREATERTHAN;
    }

    /* equal */
    return VALUE_COMPARE_EQUAL;
  }

  /*
   * Copy constructor
   */
  TupleKeyComparatorRaw(const TupleKeyComparatorRaw &) {}
  TupleKeyComparatorRaw() {};
};

class TupleKeyEqualityChecker {
 public:
  // return true if lhs == rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.GetTupleForComparison(lhs.key_tuple_schema);
    storage::Tuple rhTuple = rhs.GetTupleForComparison(rhs.key_tuple_schema);
    auto schema = lhs.key_tuple_schema;

    for (unsigned int col_itr = 0; col_itr < schema->GetColumnCount();
         ++col_itr) {
      common::Value lhValue = (
          lhTuple.GetValue(lhs.ColumnForIndexColumn(col_itr)));
      common::Value rhValue = (
          rhTuple.GetValue(rhs.ColumnForIndexColumn(col_itr)));

      common::Value res = (lhValue.CompareNotEquals(rhValue));
      if (res.IsTrue())
        return false;
    }
    return true;
  }

  TupleKeyEqualityChecker(const TupleKeyEqualityChecker &) {}
  TupleKeyEqualityChecker() {}
};

}  // End index namespace
}  // End peloton namespace
