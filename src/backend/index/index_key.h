/*-------------------------------------------------------------------------
 *
 * index_key.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/index/index_key.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "backend/common/value_peeker.h"
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
 * the typeMaxValue template parameter.
 */
template<typename signedType, int64_t typeMaxValue>
inline static signedType convertUnsignedValueToSignedValue(uint64_t value) {
  int64_t retval = static_cast<int64_t>(value);
  retval -= typeMaxValue + 1;
  return static_cast<signedType>(retval);
}

/*
 * Specialization of convertUnsignedValueToSignedValue for int64_t. int64_t requires a comparison
 * to prevent overflow.
 */
template<>
inline int64_t convertUnsignedValueToSignedValue< int64_t, INT64_MAX>(uint64_t value) {
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
 * Convert from a signed value to an unsigned value. The max value for the type is supplied as a template
 * parameter. int64_t is used for all types to prevent overflow.
 */
template<int64_t typeMaxValue, typename signedValueType, typename unsignedValueType>
inline static unsignedValueType convertSignedValueToUnsignedValue(signedValueType value) {

  return static_cast<unsignedValueType>(value + typeMaxValue + 1);
}

/*
 * Specialization for int64_ts necessary to prevent overflow.
 */
template<>
inline uint64_t convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(int64_t value) {
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
 *  Integer key that will pack all key data into keySize number of uint64_t.
 *  The minimum number of uint64_ts necessary to pack all the integers is used.
 */
template <std::size_t keySize>
class IntsKey {
 public:


  /*
   * Take a value that is part of the key (already converted to a uint64_t) and inserts it into the
   * most significant bytes available in the key. Templated on the size of the type of key being inserted.
   * This allows the compiler to unroll the loop.
   *
   *
   * Algorithm is:
   * Start with the most significant byte of the keyValue we are inserting and put into the most significant byte
   * of the uint64_t portion of the key that is indexed by keyOffset. The most significant available byte within
   * the uint64_t portion of the key is indexed by intraKeyOffset. Both keyOffset and intraKeyOffset are passed
   * by reference so they can be updated.
   *
   *
   * Rinse and repeat with the less significant bytes.
   *
   */
  template <typename keyValueType>
  inline void insertKeyValue(int &keyOffset, int &intraKeyOffset, uint64_t keyValue) {
    for (int ii = static_cast<int>(sizeof(keyValueType)) - 1; ii >= 0; ii--) {

      /*
       * Extract the most significant byte from keyValue by shifting it all the way to the right.
       * Mask off the rest. Then shift it to the left to the most significant byte location available
       * in the key and OR it in.
       */
      data[keyOffset] |= (0xFF & (keyValue >> (ii * 8))) << (intraKeyOffset * 8); //
      intraKeyOffset--;//Move the offset inside the uint64 key back one.
      /*
       * If there are no more bytes available in the uint64_t indexed by keyOffset then increment keyOffset
       * to point to the next uint64_t and set intraKeyOffset to index to the most significant byte
       * in this next uint64_t.
       */
      if (intraKeyOffset < 0) {
        intraKeyOffset = sizeof(uint64_t) - 1;
        keyOffset++;
      }
    }
  }

  /*
   * Inverse of insertKeyValue.
   */
  template <typename keyValueType>
  inline uint64_t extractKeyValue(int &keyOffset, int &intraKeyOffset) const {
    uint64_t retval = 0;
    for (int ii = static_cast<int>(sizeof(keyValueType)) - 1; ii >= 0; ii--) {
      retval |= (0xFF & (data[keyOffset] >> (intraKeyOffset * 8))) << (ii * 8);
      intraKeyOffset--;
      if (intraKeyOffset < 0) {
        intraKeyOffset = sizeof(uint64_t) - 1;
        keyOffset++;
      }
    }
    return retval;
  }

  std::string debug( const catalog::Schema *keySchema) const {
    std::ostringstream buffer;
    int keyOffset = 0;
    int intraKeyOffset = sizeof(uint64_t) - 1;
    const int GetColumnCount = keySchema->GetColumnCount();
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch(keySchema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const uint64_t keyValue = extractKeyValue<uint64_t>(keyOffset, intraKeyOffset);
          buffer << convertUnsignedValueToSignedValue< int64_t, INT64_MAX>(keyValue) << ",";
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const uint64_t keyValue = extractKeyValue<uint32_t>(keyOffset, intraKeyOffset);
          buffer << convertUnsignedValueToSignedValue< int32_t, INT32_MAX>(keyValue) << ",";
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const uint64_t keyValue = extractKeyValue<uint16_t>(keyOffset, intraKeyOffset);
          buffer << convertUnsignedValueToSignedValue< int16_t, INT16_MAX>(keyValue) << ",";
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const uint64_t keyValue = extractKeyValue<uint8_t>(keyOffset, intraKeyOffset);
          buffer << static_cast<int64_t>(convertUnsignedValueToSignedValue< int8_t, INT8_MAX>(keyValue)) << ",";
          break;
        }
        default:
          throw IndexException("We currently only support a specific set of column index sizes...");
          break;
      }
    }
    return std::string(buffer.str());
  }

  inline void setFromKey(const storage::Tuple *tuple) {
    ::memset(data, 0, keySize * sizeof(uint64_t));
    assert(tuple);
    const catalog::Schema *keySchema = tuple->GetSchema();
    const int GetColumnCount = keySchema->GetColumnCount();
    int keyOffset = 0;
    int intraKeyOffset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch(keySchema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const int64_t value = ValuePeeker::PeekBigInt(tuple->GetValue(ii));
          const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
          insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const int32_t value = ValuePeeker::PeekInteger(tuple->GetValue(ii));
          const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
          insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const int16_t value = ValuePeeker::PeekSmallInt(tuple->GetValue(ii));
          const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
          insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const int8_t value = ValuePeeker::PeekTinyInt(tuple->GetValue(ii));
          const uint8_t keyValue = convertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(value);
          insertKeyValue<uint8_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        default:
          throw IndexException("We currently only support a specific set of column index sizes...");
          break;
      }
    }
  }

  inline void setFromTuple(const storage::Tuple *tuple, const int *indices, const catalog::Schema *keySchema) {
    ::memset(data, 0, keySize * sizeof(uint64_t));
    const int GetColumnCount = keySchema->GetColumnCount();
    int keyOffset = 0;
    int intraKeyOffset = sizeof(uint64_t) - 1;
    for (int ii = 0; ii < GetColumnCount; ii++) {
      switch(keySchema->GetColumn(ii).column_type) {
        case VALUE_TYPE_BIGINT: {
          const int64_t value = ValuePeeker::PeekBigInt(tuple->GetValue(indices[ii]));
          const uint64_t keyValue = convertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(value);
          insertKeyValue<uint64_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_INTEGER: {
          const int32_t value = ValuePeeker::PeekInteger(tuple->GetValue(indices[ii]));
          const uint32_t keyValue = convertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(value);
          insertKeyValue<uint32_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_SMALLINT: {
          const int16_t value = ValuePeeker::PeekSmallInt(tuple->GetValue(indices[ii]));
          const uint16_t keyValue = convertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(value);
          insertKeyValue<uint16_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        case VALUE_TYPE_TINYINT: {
          const int8_t value = ValuePeeker::PeekTinyInt(tuple->GetValue(indices[ii]));
          const uint8_t keyValue = convertSignedValueToUnsignedValue<INT8_MAX, int8_t, uint8_t>(value);
          insertKeyValue<uint8_t>( keyOffset, intraKeyOffset, keyValue);
          break;
        }
        default:
          throw IndexException( "We currently only support a specific set of column index sizes..." );
          break;
      }
    }
  }

  // actual location of data
  uint64_t data[keySize];

 private:

};

/** comparator for Int specialized indexes. */
template <std::size_t keySize>
class IntsComparator {
 public:
  const catalog::Schema *m_keySchema;

  IntsComparator(index::IndexMetadata *metadata) : m_keySchema(metadata->GetKeySchema()) {}

  inline bool operator()(const IntsKey<keySize> &lhs, const IntsKey<keySize> &rhs) const {
    // lexographical compare could be faster for fixed N
    /*
     * Hopefully the compiler can unroll this loop
     */
    for (unsigned int ii = 0; ii < keySize; ii++) {
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
};

/**
 *
 */
template <std::size_t keySize>
class IntsEqualityChecker {
 public:

  const catalog::Schema *m_keySchema;

  IntsEqualityChecker(index::IndexMetadata *metadata) : m_keySchema(metadata->GetKeySchema()) {}

  inline bool operator()(const IntsKey<keySize> &lhs, const IntsKey<keySize> &rhs) const {
    for (unsigned int ii = 0; ii < keySize; ii++) {
      const uint64_t *lvalue = &lhs.data[ii];
      const uint64_t *rvalue = &rhs.data[ii];

      if (*lvalue != *rvalue) {
        return false;
      }
    }
    return true;
  }
};

/**
 *
 */
template <std::size_t keySize>
struct IntsHasher : std::unary_function<IntsKey<keySize>, std::size_t>
{
  IntsHasher(catalog::Schema *keySchema) {}

  inline size_t operator()(IntsKey<keySize> const& p) const
  {
    size_t seed = 0;
    for (int ii = 0; ii < keySize; ii++) {
      boost::hash_combine(seed, p.data[ii]);
    }
    return seed;
  }
};

/**
 * Key object for indexes of mixed types.
 * Using storage::Tuple to store columns.
 */
template <std::size_t keySize>
class GenericKey {
 public:
  inline void setFromKey(const storage::Tuple *tuple) {
    assert(tuple);
    ::memcpy(data, tuple->GetData(), tuple->GetSchema()->GetLength());
  }

  inline void setFromTuple(const storage::Tuple *tuple, const int *indices, const catalog::Schema *keySchema) {
    storage::Tuple keyTuple(keySchema);
    keyTuple.MoveToTuple(reinterpret_cast<void*>(data));
    for (int i = 0; i < keySchema->GetColumnCount(); i++) {
      keyTuple.SetValue(i, tuple->GetValue(indices[i]));
    }
  }

  // actual location of data, extends past the end.
  char data[keySize];

 private:

};

/**
 * Function object returns true if lhs < rhs, used for trees
 */
template <std::size_t keySize>
class GenericComparator {
 public:
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericComparator(index::IndexMetadata *metadata) : m_schema(metadata->GetKeySchema()) {}

  inline bool operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
    storage::Tuple lhTuple(m_schema); lhTuple.MoveToTuple(reinterpret_cast<const void*>(&lhs));
    storage::Tuple rhTuple(m_schema); rhTuple.MoveToTuple(reinterpret_cast<const void*>(&rhs));
    // lexographical compare could be faster for fixed N
    int diff = lhTuple.Compare(rhTuple);
    return diff < 0;
  }

  const catalog::Schema *m_schema;
};

/**
 * Equality-checking function object
 */
template <std::size_t keySize>
class GenericEqualityChecker {
 public:
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericEqualityChecker(index::IndexMetadata *metadata) : m_schema(metadata->GetKeySchema()) {}

  inline bool operator()(const GenericKey<keySize> &lhs, const GenericKey<keySize> &rhs) const {
    storage::Tuple lhTuple(m_schema); lhTuple.MoveToTuple(reinterpret_cast<const void*>(&lhs));
    storage::Tuple rhTuple(m_schema); rhTuple.MoveToTuple(reinterpret_cast<const void*>(&rhs));
    return lhTuple.EqualsNoSchemaCheck(rhTuple);
  }

  const catalog::Schema *m_schema;
};

/**
 * Hash function object for an array of SlimValues
 */
template <std::size_t keySize>
struct GenericHasher : std::unary_function<GenericKey<keySize>, std::size_t>
{
  /** Type information passed to the constuctor as it's not in the key itself */
  GenericHasher(index::IndexMetadata *metadata) : m_schema(metadata->GetKeySchema()) {}

  /** Generate a 64-bit number for the key value */
  inline size_t operator()(GenericKey<keySize> const &p) const
  {
    storage::Tuple pTuple(m_schema); pTuple.MoveToTuple(reinterpret_cast<const void*>(&p));
    return pTuple.HashCode();
  }

  const catalog::Schema *m_schema;
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
    m_columnIndices = NULL;
    m_keyTuple = NULL;
    m_keyTupleSchema = NULL;
  }

  // Set a key from a key-schema tuple.
  inline void setFromKey(const storage::Tuple *tuple) {
    assert(tuple);
    m_columnIndices = NULL;
    m_keyTuple = tuple->GetData();
    m_keyTupleSchema = tuple->GetSchema();
  }

  // Set a key from a table-schema tuple.
  inline void setFromTuple(const storage::Tuple *tuple, const int *indices, const catalog::Schema *keySchema) {
    assert(tuple);
    assert(indices);
    m_columnIndices = indices;
    m_keyTuple = tuple->GetData();
    m_keyTupleSchema = tuple->GetSchema();
  }

  // Return true if the TupleKey references an ephemeral index key.
  bool isKeySchema() const {
    return m_columnIndices == NULL;
  }

  // Return a table tuple that is valid for comparison
  storage::Tuple getTupleForComparison() const {
    return storage::Tuple(m_keyTupleSchema, m_keyTuple);
  }

  // Return the indexColumn'th key-schema column.
  int columnForIndexColumn(int indexColumn) const {
    if (isKeySchema())
      return indexColumn;
    else
      return m_columnIndices[indexColumn];
  }

 private:
  // TableIndex owns this array - NULL if an ephemeral key
  const int* m_columnIndices;

  // Pointer a persistent tuple in non-ephemeral case.
  char *m_keyTuple;
  const catalog::Schema *m_keyTupleSchema;
};

class TupleKeyComparator {
 public:
  TupleKeyComparator(index::IndexMetadata *metadata) : m_schema(metadata->GetKeySchema()) {
  }

  // return true if lhs < rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.getTupleForComparison();
    storage::Tuple rhTuple = rhs.getTupleForComparison();
    Value lhValue, rhValue;

    //std::cout << std::endl << "TupleKeyComparator: " <<
    //    std::endl << lhTuple.debugNoHeader() <<
    //    std::endl << rhTuple.debugNoHeader() <<
    //    std::endl;

    for (int ii=0; ii < m_schema->GetColumnCount(); ++ii) {
      lhValue = lhTuple.GetValue(lhs.columnForIndexColumn(ii));
      rhValue = rhTuple.GetValue(rhs.columnForIndexColumn(ii));

      int comparison = lhValue.Compare(rhValue);

      if (comparison == VALUE_COMPARE_LESSTHAN) {
        // std::cout << " LHS " << lhValue.debug() << " < RHS. " << rhValue.debug() << std::endl;
        return true;
      }
      else if (comparison == VALUE_COMPARE_GREATERTHAN) {
        // std::cout << " LHS " << lhValue.debug() << " > RHS. " << rhValue.debug() << std::endl;
        return false;
      }
    }
    // std::cout << " LHS == RHS. " << std::endl;
    return false;
  }

  const catalog::Schema *m_schema;
};

class TupleKeyEqualityChecker {
 public:
  TupleKeyEqualityChecker(index::IndexMetadata *metadata) : m_schema(metadata->GetKeySchema()) {
  }

  // return true if lhs == rhs
  inline bool operator()(const TupleKey &lhs, const TupleKey &rhs) const {
    storage::Tuple lhTuple = lhs.getTupleForComparison();
    storage::Tuple rhTuple = rhs.getTupleForComparison();
    Value lhValue, rhValue;

    //         std::cout << std::endl << "TupleKeyEqualityChecker: " <<
    //         std::endl << lhTuple.debugNoHeader() <<
    //         std::endl << rhTuple.debugNoHeader() <<
    //         std::endl;

    for (int ii=0; ii < m_schema->GetColumnCount(); ++ii) {
      lhValue = lhTuple.GetValue(lhs.columnForIndexColumn(ii));
      rhValue = rhTuple.GetValue(rhs.columnForIndexColumn(ii));

      if (lhValue.Compare(rhValue) != VALUE_COMPARE_EQUAL) {
        return false;
      }
    }
    return true;
  }

  const catalog::Schema *m_schema;
};

}  // End index namespace
}  // End peloton namespace

