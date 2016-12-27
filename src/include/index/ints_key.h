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
 * class IntsKey - Compact representation of integers of different length
 * 
 * This class is used for storing multiple integral fields into a compact
 * array representation. This class is largely used as a static object, 
 * because special storage format is used to ensure a fast comparison 
 * implementation.
 *
 * Integers are stored in a big-endian and sign-magnitute format. Big-endian
 * favors comparison since we could always start comparison using the first few
 * bytes. This gives the compiler opportunities to optimize comparison
 * using advanced techniques such as SIMD or loop unrolling. 
 *
 * For details of how and why integers must be stored in a big-endian and 
 * sign-magnitude format, please refer to adaptive radix tree's key format
 *
 * Note: IntsKey is always word-aligned on x86-64. KeySize is the number of 
 * 64 bit words inside the key, not byte size
 */
template <size_t KeySize>
class IntsKey {
 private:
  // This is the actual byte size of the key
  static constexpr size_t key_size_byte = KeySize * 8UL;
  
  // This is the array we use for storing integers
  unsigned char key_data[key_size_byte];
 
 private:
  
  /*
   * TwoBytesToBigEndian() - Change 2 bytes to big endian
   *
   * This function could be achieved using XCHG instruction; so do not write
   * your own
   *
   * i.e. MOV AX, WORD PTR [data]
   *      XCHG AH, AL
   */
  inline static uint16_t TwoBytesToBigEndian(uint16_t data) {
    return htobe16(data);  
  }
  
  /*
   * FourBytesToBigEndian() - Change 4 bytes to big endian format
   *
   * This function uses BSWAP instruction in one atomic step; do not write
   * your own
   *
   * i.e. MOV EAX, WORD PTR [data]
   *      BSWAP EAX
   */
  inline static uint32_t FourBytesToBigEndian(uint32_t data) {
    return htobe32(data); 
  }
  
  /*
   * EightBytesToBigEndian() - Change 8 bytes to big endian format
   *
   * This function uses BSWAP instruction
   */
  inline static uint64_t EightBytesToBigEndian(uint64_t data) {
    return htobe64(data);
  }  
  
  /*
   * TwoBytesToHostEndian() - Converts back two byte integer to host byte order
   */
  inline static uint16_t TwoBytesToHostEndian(uint16_t data) {
    return be16toh(data); 
  }
  
  /*
   * FourBytesToHostEndian() - Converts back four byte integer to host byte order
   */
  inline static uint32_t FourBytesToHostEndian(uint32_t data) {
    return be32toh(data); 
  }
  
  /*
   * EightBytesToHostEndian() - Converts back eight byte integer to host byte order
   */
  inline static uint64_t EightBytesToHostEndian(uint64_t data) {
    return be64toh(data); 
  }
  
  /*
   * ToBigEndian() - Overloaded version for all kinds of integral data types
   */
  
  inline static uint8_t ToBigEndian(uint8_t data) {
    return data;
  }
  
  inline static uint8_t ToBigEndian(int8_t data) {
    return static_cast<uint8_t>(data);
  }
  
  inline static uint16_t ToBigEndian(uint16_t data) {
    return TwoBytesToBigEndian(data);
  }
  
  inline static uint16_t ToBigEndian(int16_t data) {
    return TwoBytesToBigEndian(static_cast<uint16_t>(data));
  }
  
  inline static uint32_t ToBigEndian(uint32_t data) {
    return FourBytesToBigEndian(data);
  }
  
  inline static uint32_t ToBigEndian(int32_t data) {
    return FourBytesToBigEndian(static_cast<uint32_t>(data));
  }
  
  inline static uint64_t ToBigEndian(uint64_t data) {
    return EightBytesToBigEndian(data);
  }
  
  inline static uint64_t ToBigEndian(int64_t data) {
    return EightBytesToBigEndian(static_cast<uint64_t>(data));
  }
  
  /*
   * ToHostEndian() - Converts big endian data to host format
   */
  
  static inline uint8_t ToHostEndian(uint8_t data) {
    return data; 
  }
  
  static inline uint8_t ToHostEndian(int8_t data) {
    return static_cast<uint8_t>(data); 
  }
  
  static inline uint16_t ToHostEndian(uint16_t data) {
    return TwoBytesToHostEndian(data);
  }
  
  static inline uint16_t ToHostEndian(int16_t data) {
    return TwoBytesToHostEndian(static_cast<uint16_t>(data));
  }
  
  static inline uint32_t ToHostEndian(uint32_t data) {
    return FourBytesToHostEndian(data);
  }
  
  static inline uint32_t ToHostEndian(int32_t data) {
    return FourBytesToHostEndian(static_cast<uint32_t>(data));
  }
  
  static inline uint64_t ToHostEndian(uint64_t data) {
    return EightBytesToHostEndian(data);
  }
  
  static inline uint64_t ToHostEndian(int64_t data) {
    return EightBytesToHostEndian(static_cast<uint64_t>(data));
  }
  
  /*
   * SignFlip() - Flips the highest bit of a given integral type
   *
   * This flip is logical, i.e. it happens on the logical highest bit of an 
   * integer. The actual position on the address space is related to endianess
   * Therefore this should happen first.
   *
   * It does not matter whether IntType is signed or unsigned because we do
   * not use the sign bit
   */
  template <typename IntType>
  inline static IntType SignFlip(IntType data) {
    // This sets 1 on the MSB of the corresponding type
    // NOTE: Must cast 0x1 to the correct type first
    // otherwise, 0x1 is treated as the signed int type, and after leftshifting
    // if it is extended to larger type then sign extension will be used
    IntType mask = static_cast<IntType>(0x1) << (sizeof(IntType) * 8UL - 1);
    
    return data ^ mask;
  }
  
 public:
  
  /*
   * Constructor
   */
  IntsKey() {
    ZeroOut();
    
    return;
  }
  
  /*
   * ZeroOut() - Sets all bits to zero
   */
  inline void ZeroOut() {
    memset(key_data, 0x00, key_size_byte);
    
    return;
  }
  
  /*
   * AddInteger() - Adds a new integer into the compact form
   *
   * Note that IntType must be of the following 8 types:
   *   int8_t; int16_t; int32_t; int64_t
   * Otherwise the result is undefined
   */
  template <typename IntType>
  inline void AddInteger(IntType data, size_t offset) {
    IntType sign_flipped = SignFlip<IntType>(data);
    
    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(sign_flipped);
    
    // This will almost always be optimized into single move
    memcpy(key_data + offset, &big_endian, sizeof(IntType));
    
    return;
  }
  
  /*
   * AddUnsignedInteger() - Adds an unsigned integer of a certain type
   *
   * Only the following unsigned type should be used:
   *   uint8_t; uint16_t; uint32_t; uint64_t
   */
  template <typename IntType>
  inline void AddUnsignedInteger(IntType data, size_t offset) {
    // This function always returns the unsigned type
    // so we must use automatic type inference
    auto big_endian = ToBigEndian(data);
    
    // This will almost always be optimized into single move
    memcpy(key_data + offset, &big_endian, sizeof(IntType));
    
    return;
  }
  
  /*
   * GetInteger() - Extracts an integer from the given offset
   *
   * This function has the same limitation as stated for AddInteger()
   */
  template <typename IntType>
  inline IntType GetInteger(size_t offset) {
    const IntType *ptr = reinterpret_cast<IntType *>(key_data + offset);
    
    // This always returns an unsigned number
    auto host_endian = ToHostEndian(*ptr);
    
    return SignFlip<IntType>(static_cast<IntType>(host_endian));
  }
  
  /*
   * GetUnsignedInteger() - Extracts an unsigned integer from the given offset
   *
   * The same constraint about IntType applies
   */
  template <typename IntType>
  inline IntType GetUnsignedInteger(size_t offset) {
    const IntType *ptr = reinterpret_cast<IntType *>(key_data + offset);
    auto host_endian = ToHostEndian(*ptr);
    return static_cast<IntType>(host_endian);
  }
  
  /*
   * Compare() - Compares two IntsType object of the same length
   *
   * This function has the same semantics as memcmp(). Negative result means 
   * less than, positive result means greater than, and 0 means equal
   */
  static inline int Compare(const IntsKey<KeySize> &a, 
                            const IntsKey<KeySize> &b) {
    return memcmp(a.key_data, b.key_data, IntsKey<KeySize>::key_size_byte); 
  }
  
  /*
   * LessThan() - Returns true if first is less than the second
   */
  static inline bool LessThan(const IntsKey<KeySize> &a, 
                              const IntsKey<KeySize> &b) {
    return Compare(a, b) < 0;
  }
  
  /*
   * Equals() - Returns true if first is equivalent to the second
   */
  static inline bool Equals(const IntsKey<KeySize> &a, 
                            const IntsKey<KeySize> &b) {
    return Compare(a, b) == 0;
  }
  
 public:
  
  /*
   * PrintRawData() - Prints the content of this key
   *
   * All contents are printed to stderr
   */
  void PrintRawData() {
    // This is the current offset we are on printing the key
    int offset = 0;
    
    fprintf(stderr, "IntsKey<%lu> - %lu bytes\n", KeySize, key_size_byte);
    
    while(offset < key_size_byte) {
      constexpr int byte_per_line = 16;
      
      fprintf(stderr, "0x%.8X    ", offset);
      
      for(int i = 0;i < byte_per_line;i++) {
        if(offset >= key_size_byte) {
          break;
        } 
        
        fprintf(stderr, "%.2X ", key_data[offset]);
        
        // Add a delimiter on the 8th byte
        if(i == 7) {
          fprintf(stderr, "   "); 
        }
        
        offset++; 
      }
      
      fprintf(stderr, "\n");
    }
    
    return;
  }
};

/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////
 

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
   *   2. Caller initializes key_offset and intra_key_offset to 0 and 
   *      sizeof(uint64_t) - 1 (i.e. highest byte in a uint64_t)
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
        case type::Type::BIGINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint64_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int64_t, INT64_MAX>(
                        key_value) << ",";
          break;
        }
        case type::Type::INTEGER: {
          const uint64_t key_value =
              ExtractKeyValue<uint32_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int32_t, INT32_MAX>(
                        key_value) << ",";
          break;
        }
        case type::Type::SMALLINT: {
          const uint64_t key_value =
              ExtractKeyValue<uint16_t>(key_offset, intra_key_offset);
          buffer << ConvertUnsignedValueToSignedValue<int16_t, INT16_MAX>(
                        key_value) << ",";
          break;
        }
        case type::Type::TINYINT: {
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
   *
   * Since we assume this tuple only contains key columns and there is no
   * other column, it is not necessary to specify a vector of object IDs
   * to indicate index column
   */
  inline void SetFromKey(const storage::Tuple *tuple) {
    // Must clear previous result first
    PL_MEMSET(data, 0, KeySize * sizeof(uint64_t));
    
    PL_ASSERT(tuple);
    
    // This returns schema of the tuple
    // Note that the schema must contain only integral type
    const catalog::Schema *key_schema = tuple->GetSchema();
    
    // Need this to loop through columns
    const int column_count = key_schema->GetColumnCount();
    
    // Init start with the first uint64_t element in data array, and also
    // start with the highest byte of the integer such that comparison using
    // the integer yields correct result
    int key_offset = 0;
    int intra_key_offset = sizeof(uint64_t) - 1;
    
    // Loop from most significant column to least significant column
    for (int ii = 0; ii < column_count; ii++) {
      switch (key_schema->GetColumn(ii).column_type) {
        case type::Type::BIGINT: {
          type::Value val = tuple->GetValue(ii);
          const int64_t value = type::ValuePeeker::PeekBigInt(val);
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::INTEGER: {
          type::Value val = tuple->GetValue(ii);
          const int32_t value = type::ValuePeeker::PeekInteger(val);
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::SMALLINT: {
          type::Value val = tuple->GetValue(ii);
          const int16_t value = type::ValuePeeker::PeekSmallInt(val);
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::TINYINT: {
          type::Value val = tuple->GetValue(ii);
          const int8_t value = type::ValuePeeker::PeekTinyInt(val);
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
        case type::Type::BIGINT: {
          type::Value val = tuple->GetValue(indices[ii]);
          const int64_t value = type::ValuePeeker::PeekBigInt(val);
          const uint64_t key_value =
              ConvertSignedValueToUnsignedValue<INT64_MAX, int64_t, uint64_t>(
                  value);
          InsertKeyValue<uint64_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::INTEGER: {
          type::Value val = tuple->GetValue(indices[ii]);
          const int32_t value =
              type::ValuePeeker::PeekInteger(val);
          const uint32_t key_value =
              ConvertSignedValueToUnsignedValue<INT32_MAX, int32_t, uint32_t>(
                  value);
          InsertKeyValue<uint32_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::SMALLINT: {
          type::Value val = tuple->GetValue(indices[ii]);
          const int16_t value = type::ValuePeeker::PeekSmallInt(val);
          const uint16_t key_value =
              ConvertSignedValueToUnsignedValue<INT16_MAX, int16_t, uint16_t>(
                  value);
          InsertKeyValue<uint16_t>(key_offset, intra_key_offset, key_value);
          break;
        }
        case type::Type::TINYINT: {
          type::Value val = tuple->GetValue(indices[ii]);
          const int8_t value = type::ValuePeeker::PeekTinyInt(val);
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
