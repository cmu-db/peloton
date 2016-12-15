//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ints_key.h
//
// Identification: src/include/index/ints_key.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

namespace peloton {
namespace index {

/*
 * ConvertUnsignedValueToSignedValue() - As name suggests
 *
 * Convert from a uint64_t that has had a signed number packed into it to
 * the specified signed type. The max signed value for that type is supplied as
 * the TargetTypeMaxValue template parameter.
 */
template <typename TargetType, 
          int64_t TargetTypeMaxValue>
inline static TargetType ConvertUnsignedValueToSignedValue(uint64_t value) {
  return static_cast<TargetType>(value - TargetTypeMaxValue + 1UL);
}

/*
 * ConvertUnsignedValueToSignedValue() - As name suggests
 * 
 * This function is a template specialization of 
 * ConvertUnsignedValueToSignedValue with int64_t, since with int64_t 
 * computation it is possible that overflow appearing
 */
template <>
inline int64_t 
ConvertUnsignedValueToSignedValue<int64_t, INT64_MAX>(uint64_t value) {
  return (value > static_cast<uint64_t>(INT64_MAX) + 1) ? \
         (static_cast<int64_t>(value - INT64_MAX - 1)) : \
         (static_cast<int64_t>(value) - INT64_MAX - 1);
}

/*
 * ConvertSignedValueToUnsignedValue() - As name suggests
 *
 * Convert from a signed value to an unsigned value. The max value for the type
 * is supplied as a template
 * parameter. int64_t is used for all types to prevent overflow.
 */
template <int64_t TypeMaxValue, 
          typename SignedValueType,
          typename UnsignedValueType>
inline static 
UnsignedValueType ConvertSignedValueToUnsignedValue(SignedValueType value) {
  return static_cast<UnsignedValueType>(value + TypeMaxValue + 1);
}

/*
 * ConvertSignedValueToUnsignedValue() - As name suggests
 *
 * This is a template specialization to prevent integer oveflow for 
 * int64_t value type.
 */
template <>
inline uint64_t 
ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(int64_t value) {
  return (value < 0) ? \
         (static_cast<uint64_t>(value + INT64_MAX + 1)) : \
         (static_cast<uint64_t>(value) + INT64_MAX + 1);
}

/*
 * class IntsKey - Integer key
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
   * InsertKeyValue() - Adding bytes from a key into the data array 
   *
   * This function assumes:
   *   1. The length of key is always aligned to bytes
   *   2. Caller initializes key_offset and intra_key_offset to both 0
   *      and will not change it outside this function; they will be
   *      modified inside this function
   *   3. Even if length of the key exceeds uint64_t size, users cuold only
   *      pass in uint64_t, representing MSB of the key
   *   4. Length of the entire key is fixed, but whenever we push a key into 
   *      the key we could change it
   *   5. KeySize is the number of uint64_t in this object, not bytes
   *
   * This function uses the data array as a big-endian huge integre. Bytes from
   * key_value will be pushed into the data array byte wise from MSB to LSB
   */
  template <typename KeyValueType>
  inline void InsertKeyValue(int &key_offset, 
                             int &intra_key_offset,
                             uint64_t key_value) {
                               
    // Loop from the MSB to LSB of the key value and extract each byte
    // bytes will be pushed into the data array one by one
    for (int ii = static_cast<int>(sizeof(KeyValueType)) - 1; ii >= 0; ii--) {
      // Extract the current byte we are working on
      // Since ii always drcreses from high low, we extract MSB first
      uint64_t current_byte = (0xFF & (key_value >> (ii * 8)));
      
      // This is the byte in the current element of uint64_t array
      uint64_t byte_in_data = current_byte << (intra_key_offset * 8);
      
      // Plug the byte in data into the array
      data[key_offset] |= byte_in_data;
                         
      // Adjust the next byte we will put into uint64_t (we also put into
      // uint64_t from high to low) 
      intra_key_offset--;
      
      // If the current uint64_t runs out we just switch to the next uint64_t
      if (intra_key_offset < 0) {
        intra_key_offset = sizeof(uint64_t) - 1;
        key_offset++;
      }
    }
    
    return;
  }

  /*
   * ExtractKeyValue() - This function does the reverse of InsertKeyValue()
   *                     and fills in a uint64_t with bytes from the array
   */
  template <typename KeyValueType>
  inline uint64_t ExtractKeyValue(int &key_offset,
                                  int &intra_key_offset) const {
    // Must set it to 0 first
    uint64_t retval = 0x0;
    
    // Loop from the MSB to LSB for return value
    for (int ii = static_cast<int>(sizeof(KeyValueType)) - 1; ii >= 0; ii--) {
      // Note that the precedence of & is lower than >> and <<
      uint64_t current_byte = \
        0xFF & ((data[key_offset] >> (intra_key_offset * 8)));
        
      // Get the byte to MSB first and then LSB between iterations
      uint64_t byte_in_result = current_byte << (ii * 8); 
      
      // Plug byte in the result
      retval |= byte_in_result;
      
      // Go to lower byte in data array
      intra_key_offset--;
      
      // If we have run out of bytes in the current uint64_t just go to
      // the next uint64_t
      if (intra_key_offset < 0) {
        intra_key_offset = sizeof(uint64_t) - 1;
        key_offset++;
      }
    }
    
    return retval;
  }

  /*
   * GetTupleForComparison() - This is not supported yet
   */
  const storage::Tuple GetTupleForComparison(
      UNUSED_ATTRIBUTE const catalog::Schema *key_schema) const {
    throw IndexException("Tuple conversion not supported");
  }

  /*
   * Debug() - Get a debugging string from the object
   */
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

  /*
   * SetFromKey() - Sets the compact internal storage from a tuple
   *                only comtaining key columns
   */
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

}  // End index namespace
}  // End peloton namespace
