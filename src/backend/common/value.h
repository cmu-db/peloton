//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value.h
//
// Identification: src/backend/common/value.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <cfloat>
#include <climits>
#include <cmath>
#include <cstring>
#include <exception>
#include <limits>
#include <stdint.h>
#include <string>
#include <algorithm>
#include <iostream>

#include "ttmath/ttmathint.h"

#include "backend/common/exception.h"
#include "backend/common/pool.h"
#include "backend/common/serializer.h"
#include "backend/common/types.h"
#include "backend/common/varlen.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Type system
//===--------------------------------------------------------------------===//

#define CHECK_FPE(x) (std::isinf(x) || std::isnan(x))

/**
 * Objects are length preceded with a short length value or a long length value
 * depending on how many bytes are needed to represent the length. These
 * define how many bytes are used for the short value vs. the long value.
 */
#define SHORT_OBJECT_LENGTHLENGTH static_cast<char>(1)
#define LONG_OBJECT_LENGTHLENGTH static_cast<char>(4)
#define OBJECT_NULL_BIT static_cast<char>(1 << 6)
#define OBJECT_CONTINUATION_BIT static_cast<char>(1 << 7)
#define OBJECT_MAX_LENGTH_SHORT_LENGTH 63

// The int used for storage and return values
typedef ttmath::Int<2> TTInt;

// Long integer with space for multiplication and division without
// carry/overflow
typedef ttmath::Int<4> TTLInt;

class ValuePeeker;

/**
 * A class to wrap all scalar values regardless of type and storage.
 *
 * An Value is not the representation used in the serialization of tables
 * nor is it the representation of how scalar values are stored in tables.
 * Value does have serialization and deserialization mechanisms for both those
 * storage formats.
 *
 * Values are designed to be immutable and for the most part not
 * constructable from raw data types. Access to the raw data is restricted
 * so that all operations have to go through the member functions that can
 * perform the correct Casting and error checking.
 */
class Value {
  friend class ValuePeeker;
  friend class ValueFactory;

 public:
  /**
   * Copy assignment operator
   * @warning It performs a shallow copy. Use Clone() if a deep copy is
   * requested.
   */
  Value &operator=(const Value &) = default;

  // Create a default Value
  Value();

  // Release memory associated to object type Values
  void FreeUninlinedData() const;

  //===--------------------------------------------------------------------===//
  // Core Functions
  //===--------------------------------------------------------------------===//

  // Create an Value promoted/demoted to type
  Value CastAs(ValueType type) const;

  // Reveal the contained pointer for type values
  void *CastAsAddress() const;

  //===--------------------------------------------------------------------===//
  // NULLs and BOOLs
  //===--------------------------------------------------------------------===//

  // Set value to the correct SQL NULL representation.
  void SetNull();

  // Create a boolean true Value
  static Value GetTrue();

  // Create a boolean false Value
  static Value GetFalse();

  // Create an Value with the null representation for valueType
  static Value GetNullValue(ValueType);

  // Check if the value represents SQL NULL
  bool IsNull() const;

  // For boolean Values, convert to bool
  bool IsTrue() const;
  bool IsFalse() const;

  // For boolean Values only, logical operators
  Value OpNegate() const;
  Value OpAnd(const Value rhs) const;
  Value OpOr(const Value rhs) const;

  // Get min value
  static Value GetMinValue(ValueType);

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization utilities
  //===--------------------------------------------------------------------===//

  /**
   *  Serialize the scalar this Value represents to the storage area
   *  provided. If the scalar is an Object type then the object will
   *  be copy if it can be inlined into the tuple. Otherwise a
   *  pointer to the object will be copied into the storage area.
   *  No allocations are performed.
   *  */
  void Serialize(void *storage, const bool is_inlined,
                 const int32_t max_length) const;

  // Serialize this Value to a SerializeOutput
  void SerializeTo(SerializeOutput &output) const;

  // Serialize this Value to an Export stream
  void SerializeToExport(ExportSerializeOutput &) const;

  /**
   * Serialize the scalar this Value represents to the provided
   * storage area. If the scalar is an Object type that is not
   * inlined then the provided data pool or the heap will be used to
   * allocated storage for a copy of the object.
   * */
  void SerializeWithAllocation(void *storage, const bool is_inlined,
                               const int32_t max_length, Pool *data_pool) const;

  /**
   * Deserialize a scalar of the specified type from the tuple
   * storage area provided. If this is an Object type then the third
   * argument indicates whether the object is stored in the tuple inlined
   * */
  static const Value Deserialize(const void *storage, const ValueType type,
                                 const bool is_inlined);

  /**
   * Deserialize a scalar value of the specified type from the
   * SerializeInput directly into the tuple storage area
   * provided. This function will perform memory allocations for
   * Object types as necessary using the provided data pool or the
   * heap. This is used to deserialize tables.
   * */
  static int64_t DeserializeFrom(SerializeInput &input, const ValueType type,
                                 char *storage, bool is_inlined,
                                 const int32_t max_length, Pool *data_pool);

  /**
   * Read a ValueType from the SerializeInput stream and deserialize
   * a scalar value of the specified type from the provided
   * SerializeInput and perform allocations as necessary.
   * */
  static const Value DeserializeWithAllocation(SerializeInput &input,
                                               Pool *data_pool);

  //===--------------------------------------------------------------------===//
  // Operators
  //===--------------------------------------------------------------------===//

  /**
   *  Evaluate the ordering relation against two Values. Promotes
   *  exact types to allow disparate type comparison. See also the
   *  Op functions which return boolean Values.
   *  */
  int Compare(const Value rhs) const;

  inline bool operator==(const Value &other) const {
    if (this->Compare(other) == 0) {
      return true;
    }

    return false;
  }

  inline bool operator!=(const Value &other) const { return !(*this == other); }

  // Return a boolean Value with the comparison result
  Value OpEquals(const Value rhs) const;
  Value OpNotEquals(const Value rhs) const;
  Value OpLessThan(const Value rhs) const;
  Value OpLessThanOrEqual(const Value rhs) const;
  Value OpGreaterThan(const Value rhs) const;
  Value OpGreaterThanOrEqual(const Value rhs) const;

  // Return a copy of MAX(this, rhs)
  Value OpMax(const Value rhs) const;

  // Return a copy of MIN(this, rhs)
  Value OpMin(const Value rhs) const;

  // For number Values, compute new Values for arithmetic operators
  Value OpIncrement() const;
  Value OpDecrement() const;
  Value OpSubtract(const Value rhs) const;
  Value OpAdd(const Value rhs) const;
  Value OpMultiply(const Value rhs) const;
  Value OpDivide(const Value rhs) const;

  // For number values, check the number line.
  bool IsNegative() const;
  bool IsZero() const;

  //===--------------------------------------------------------------------===//
  // Misc functions
  //===--------------------------------------------------------------------===//

  // Calculate the tuple storage size for an Value type.
  // VARCHARs assume out-of-band tuple storage
  static uint16_t GetTupleStorageSize(const ValueType type);

  // For boost hashing
  void HashCombine(std::size_t &seed) const;

  // Get a string representation of this value
  friend std::ostream &operator<<(std::ostream &os, const Value &value);

  // Return a string full of arcane and wonder
  std::string GetInfo() const;

  // Functor comparator for use with std::set
  struct ltValue {
    bool operator()(const Value v1, const Value v2) const {
      return v1.Compare(v2) < 0;
    }
  };

  // Functor equality predicate for use with boost unordered
  struct equal_to : std::binary_function<Value, Value, bool> {
    bool operator()(Value const &x, Value const &y) const {
      return x.Compare(y) == 0;
    }
  };

  // Functor hash predicate for use with boost unordered
  struct hash : std::unary_function<Value, std::size_t> {
    std::size_t operator()(Value const &x) const {
      std::size_t seed = 0;
      x.HashCombine(seed);
      return seed;
    }
  };

  // Constants for Decimal type
  // Precision and scale (inherent in the schema)
  static const uint16_t max_decimal_precision = 38;
  static const uint16_t max_decimal_scale = 12;
  static const int64_t max_decimal_scale_factor = 1000000000000;

  int GetIntegerForTestsOnly() { return GetInteger(); }

  /**
   * Get the type of the value. This information is private
   * to prevent code outside of Value from branching based on the type of a
   * value.
   */
  ValueType GetValueType() const { return value_type; }

 private:
  //===--------------------------------------------------------------------===//
  // Function declarations for value.cpp
  //===--------------------------------------------------------------------===//

  static std::string GetTypeName(ValueType type);
  void CreateDecimalFromString(const std::string &txt);
  std::string CreateStringFromDecimal() const;

  // Promotion Rules.
  static ValueType IntPromotionTable[];
  static ValueType DecimalPromotionTable[];
  static ValueType DoublePromotionTable[];
  static TTInt MaxDecimal;
  static TTInt MinDecimal;

  static ValueType PromoteForOp(ValueType vta, ValueType vtb) {
    ValueType rt;
    switch (vta) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        rt = IntPromotionTable[vtb];
        break;

      case VALUE_TYPE_DECIMAL:
        rt = DecimalPromotionTable[vtb];
        break;

      case VALUE_TYPE_DOUBLE:
        rt = DoublePromotionTable[vtb];
        break;

      // no valid promotion (currently) for these types
      case VALUE_TYPE_ADDRESS:
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      case VALUE_TYPE_BOOLEAN:
      case VALUE_TYPE_INVALID:
      case VALUE_TYPE_NULL:
      default:
        rt = VALUE_TYPE_INVALID;
        break;
    }
    assert(rt != VALUE_TYPE_INVALID);
    return rt;
  }

  //===--------------------------------------------------------------------===//
  // Data members and their accessors
  //===--------------------------------------------------------------------===//

  /**
   * 16 bytes of storage for Value data.
   */
  char value_data[16];
  ValueType value_type;
  bool is_inlined;

  /**
   * Private constructor that initializes storage and the specifies the type of
   * value
   * that will be stored in this instance
   */
  Value(const ValueType type) {
    ::memset(value_data, 0, 16);
    SetValueType(type);
    is_inlined = false;
  }

  /**
   * Set the type of the value that will be stored in this instance.
   * The last of the 16 bytes of storage allocated in an Value
   * is used to store the type
   */
  void SetValueType(ValueType type) { value_type = type; }

  void SetSourceInlined(bool sourceInlined) { is_inlined = sourceInlined; }

  /**
   * An Object is something like a String that has a variable length
   * (thus it is length preceded) and can potentially have indirect
   * storage (will always be indirect when referenced via an Value).
   * Values cache a decoded version of the length preceding value
   * in their data area after the pointer to the object storage area.
   *
   * Leverage private access and enforce strict requirements on
   * calling correctness.
   */
  int32_t GetObjectLength() const {
    if (IsNull()) {
      // Conceptually, I think a NULL object should just have
      // length 0. In practice, this code path is often a defect
      // in code not correctly handling null. May favor a more
      // defensive "return 0" in the future? (rtb)
      throw Exception("Must not ask  for object length on sql null object.");
    }

    if ((GetValueType() != VALUE_TYPE_VARCHAR) &&
        (GetValueType() != VALUE_TYPE_VARBINARY)) {
      // probably want GetTupleStorageSize() for non-object types.
      // at the moment, only varchars are using GetObjectLength().
      throw Exception("Must not ask for object length for non-object types");
    }

    // now safe to read and return the length preceding value.
    return *reinterpret_cast<const int32_t *>(&value_data[8]);
  }

  void SetObjectLength(int32_t length) {
    *reinterpret_cast<int32_t *>(&value_data[8]) = length;
  }

  /**
   * -1 is returned for the length of a NULL object. If a string
   * is inlined in its storage location there will be no pointer to
   * check for NULL. The length preceding value must be used instead.
   *
   * The format for a length preceding value is a 1-byte short representation
   * with the the 7th bit used to indicate a null value and the 8th bit used
   * to indicate that this is part of a long representation and that 3 bytes
   * follow. 6 bits are available to represent length for a maximum length
   * of 63 bytes representable with a single byte length. 30 bits are available
   * when the continuation bit is set and 3 bytes follow.
   *
   * The value is converted to network byte order so that the code
   * will always know which byte contains the most signficant digits.
   */
  static int32_t GetObjectLengthFromLocation(const char *location) {
    /*
     * Location will be NULL if the Value is operating on storage that is not
     * inlined and thus can contain a NULL pointer
     */
    if (location == NULL) {
      return -1;
    }
    char firstByte = location[0];
    const char premask =
        static_cast<char>(OBJECT_NULL_BIT | OBJECT_CONTINUATION_BIT);

    /*
     * Generated mask that removes the null and continuation bits
     * from a single byte length value
     */
    const char mask = ~premask;
    int32_t decodedNumber = 0;
    if ((firstByte & OBJECT_NULL_BIT) != 0) {
      return -1;
    } else if ((firstByte & OBJECT_CONTINUATION_BIT) != 0) {
      char numberBytes[4];
      numberBytes[0] = static_cast<char>(location[0] & mask);
      numberBytes[1] = location[1];
      numberBytes[2] = location[2];
      numberBytes[3] = location[3];
      decodedNumber = ntohl(*reinterpret_cast<int32_t *>(numberBytes));
    } else {
      decodedNumber = location[0] & mask;
    }
    return decodedNumber;
  }

  /**
   * Retrieve the number of bytes used by the length preceding value
   * in the object's storage area. This value
   * is cached in the Value's 13th byte.
   */
  int8_t GetObjectLengthLength() const { return value_data[12]; }

  /**
   * Set the objects length preceding values length to
   * the specified value
   */
  void SetObjectLengthLength(int8_t length) { value_data[12] = length; }

  /**
   * Based on the objects actual length value Get the length of the
   * length preceding value to the appropriate length
   */
  static int8_t GetAppropriateObjectLengthLength(int32_t length) {
    if (length <= OBJECT_MAX_LENGTH_SHORT_LENGTH) {
      return SHORT_OBJECT_LENGTHLENGTH;
    } else {
      return LONG_OBJECT_LENGTHLENGTH;
    }
  }

  /**
   * Set the length preceding value using the short or long representation
   * depending
   * on what is necessary to represent the length.
   */
  static void SetObjectLengthToLocation(int32_t length, char *location) {
    int32_t beNumber = htonl(length);
    if (length < -1) {
      throw Exception("Object length cannot be < -1");
    } else if (length == -1) {
      location[0] = OBJECT_NULL_BIT;
    }
    if (length <= OBJECT_MAX_LENGTH_SHORT_LENGTH) {
      location[0] = reinterpret_cast<char *>(&beNumber)[3];
    } else {
      char *pointer = reinterpret_cast<char *>(&beNumber);
      location[0] = pointer[0];
      location[0] |= OBJECT_CONTINUATION_BIT;
      location[1] = pointer[1];
      location[2] = pointer[2];
      location[3] = pointer[3];
    }
  }

  /**
   * Not truly symmetrical with GetObjectValue which returns the actual object
   * past
   * the length preceding value
   */
  void SetObjectValue(void *object) {
    *reinterpret_cast<void **>(value_data) = object;
  }

  /**
   * Get a pointer to the value of an Object that lies beyond the storage of the
   * length information
   */
  void *GetObjectValue() const {
    if (*reinterpret_cast<void *const *>(value_data) == NULL) {
      return NULL;
    } else if (*reinterpret_cast<const int32_t *>(&value_data[8]) ==
               OBJECTLENGTH_NULL) {
      return NULL;
    } else {
      void *value;
      if (is_inlined) {
        value = *reinterpret_cast<char *const *>(value_data) +
                GetObjectLengthLength();
      } else {
        Varlen *sref = *reinterpret_cast<Varlen *const *>(value_data);
        // TODO: Peloton Changes
        // std::cout << "Varlen object stored at: " << static_cast<void *>(sref)
        // << std::endl;
        value = sref->Get() + GetObjectLengthLength();
      }
      return value;
    }
  }

  //===--------------------------------------------------------------------===//
  // Type Getters
  //===--------------------------------------------------------------------===//

  const int8_t &GetTinyInt() const {
    assert(GetValueType() == VALUE_TYPE_TINYINT);
    return *reinterpret_cast<const int8_t *>(value_data);
  }

  int8_t &GetTinyInt() {
    assert(GetValueType() == VALUE_TYPE_TINYINT);
    return *reinterpret_cast<int8_t *>(value_data);
  }

  const int16_t &GetSmallInt() const {
    assert(GetValueType() == VALUE_TYPE_SMALLINT);
    return *reinterpret_cast<const int16_t *>(value_data);
  }

  int16_t &GetSmallInt() {
    assert(GetValueType() == VALUE_TYPE_SMALLINT);
    return *reinterpret_cast<int16_t *>(value_data);
  }

  const int32_t &GetInteger() const {
    assert(GetValueType() == VALUE_TYPE_INTEGER);
    return *reinterpret_cast<const int32_t *>(value_data);
  }

  int32_t &GetInteger() {
    assert(GetValueType() == VALUE_TYPE_INTEGER);
    return *reinterpret_cast<int32_t *>(value_data);
  }

  const int64_t &GetBigInt() const {
    assert((GetValueType() == VALUE_TYPE_BIGINT) ||
           (GetValueType() == VALUE_TYPE_TIMESTAMP) ||
           (GetValueType() == VALUE_TYPE_ADDRESS));
    return *reinterpret_cast<const int64_t *>(value_data);
  }

  int64_t &GetBigInt() {
    assert((GetValueType() == VALUE_TYPE_BIGINT) ||
           (GetValueType() == VALUE_TYPE_TIMESTAMP) ||
           (GetValueType() == VALUE_TYPE_ADDRESS));
    return *reinterpret_cast<int64_t *>(value_data);
  }

  const int64_t &GetTimestamp() const {
    assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
    return *reinterpret_cast<const int64_t *>(value_data);
  }

  int64_t &GetTimestamp() {
    assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
    return *reinterpret_cast<int64_t *>(value_data);
  }

  const double &GetDouble() const {
    assert(GetValueType() == VALUE_TYPE_DOUBLE);
    return *reinterpret_cast<const double *>(value_data);
  }

  double &GetDouble() {
    assert(GetValueType() == VALUE_TYPE_DOUBLE);
    return *reinterpret_cast<double *>(value_data);
  }

  const bool &GetBoolean() const {
    assert(GetValueType() == VALUE_TYPE_BOOLEAN);
    return *reinterpret_cast<const bool *>(value_data);
  }

  TTInt &GetDecimal() {
    assert(GetValueType() == VALUE_TYPE_DECIMAL);
    void *retval = reinterpret_cast<void *>(value_data);
    return *reinterpret_cast<TTInt *>(retval);
  }

  const TTInt &GetDecimal() const {
    assert(GetValueType() == VALUE_TYPE_DECIMAL);
    const void *retval = reinterpret_cast<const void *>(value_data);
    return *reinterpret_cast<const TTInt *>(retval);
  }

  bool &GetBoolean() {
    assert(GetValueType() == VALUE_TYPE_BOOLEAN);
    return *reinterpret_cast<bool *>(value_data);
  }

  std::size_t GetAllocationSizeForObject() const;
  static std::size_t GetAllocationSizeForObject(int32_t length);

  //===--------------------------------------------------------------------===//
  // Type Cast and Getters
  //===--------------------------------------------------------------------===//

  int64_t CastAsBigIntAndGetValue() const;
  int64_t CastAsRawInt64AndGetValue() const;
  double CastAsDoubleAndGetValue() const;
  TTInt CastAsDecimalAndGetValue() const;

  Value CastAsBigInt() const;
  Value CastAsTimestamp() const;
  Value CastAsInteger() const;
  Value CastAsSmallInt() const;
  Value CastAsTinyInt() const;
  Value CastAsDouble() const;
  Value CastAsString() const;
  Value CastAsBinary() const;
  Value CastAsDecimal() const;

  //===--------------------------------------------------------------------===//
  // Type Comparison
  //===--------------------------------------------------------------------===//

  int CompareAnyIntegerValue(const Value rhs) const;
  int CompareDoubleValue(const Value rhs) const;
  int CompareStringValue(const Value rhs) const;
  int CompareBinaryValue(const Value rhs) const;
  int CompareDecimalValue(const Value rhs) const;

  //===--------------------------------------------------------------------===//
  // Type operators
  //===--------------------------------------------------------------------===//

  Value OpAddBigInts(const int64_t lhs, const int64_t rhs) const;
  Value OpSubtractBigInts(const int64_t lhs, const int64_t rhs) const;
  Value OpMultiplyBigInts(const int64_t lhs, const int64_t rhs) const;
  Value OpDivideBigInts(const int64_t lhs, const int64_t rhs) const;

  Value OpAddDoubles(const double lhs, const double rhs) const;
  Value OpSubtractDoubles(const double lhs, const double rhs) const;
  Value OpMultiplyDoubles(const double lhs, const double rhs) const;
  Value OpDivideDoubles(const double lhs, const double rhs) const;

  Value OpAddDecimals(const Value lhs, const Value rhs) const;
  Value OpSubtractDecimals(const Value lhs, const Value rhs) const;
  Value OpDivideDecimals(const Value lhs, const Value rhs) const;
  Value OpMultiplyDecimals(const Value &lhs, const Value &rhs) const;

  //===--------------------------------------------------------------------===//
  // Static Type Getters
  //===--------------------------------------------------------------------===//

  static Value GetInvalidValue() {
    Value retval(VALUE_TYPE_INVALID);
    return retval;
  }

  static Value GetTinyIntValue(int8_t value) {
    Value retval(VALUE_TYPE_TINYINT);
    retval.GetTinyInt() = value;
    return retval;
  }

  static Value GetSmallIntValue(int16_t value) {
    Value retval(VALUE_TYPE_SMALLINT);
    retval.GetSmallInt() = value;
    return retval;
  }

  static Value GetIntegerValue(int32_t value) {
    Value retval(VALUE_TYPE_INTEGER);
    retval.GetInteger() = value;
    return retval;
  }

  static Value GetBigIntValue(int64_t value) {
    Value retval(VALUE_TYPE_BIGINT);
    retval.GetBigInt() = value;
    return retval;
  }

  static Value GetTimestampValue(int64_t value) {
    Value retval(VALUE_TYPE_TIMESTAMP);
    retval.GetTimestamp() = value;
    return retval;
  }

  static Value GetDoubleValue(double value) {
    Value retval(VALUE_TYPE_DOUBLE);
    retval.GetDouble() = value;
    return retval;
  }

  static Value GetDecimalValueFromString(const std::string &value) {
    Value retval(VALUE_TYPE_DECIMAL);
    retval.CreateDecimalFromString(value);
    return retval;
  }

  static Value GetStringValue(std::string value, Pool *data_pool) {
    Value retval(VALUE_TYPE_VARCHAR);
    const int32_t length = static_cast<int32_t>(value.length());
    const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
    const int32_t minLength = length + lengthLength;
    Varlen *sref = Varlen::Create(minLength, data_pool);
    char *storage = sref->Get();
    SetObjectLengthToLocation(length, storage);
    ::memcpy(storage + lengthLength, value.c_str(), length);
    retval.SetObjectValue(sref);
    retval.SetObjectLength(length);
    retval.SetObjectLengthLength(lengthLength);
    return retval;
  }

  // Assumes binary value in hex
  static Value GetBinaryValue(const std::string value, Pool *data_pool) {
    Value retval(VALUE_TYPE_VARBINARY);
    const int32_t length = static_cast<int32_t>(value.length() / 2);

    boost::shared_array<unsigned char> buf(new unsigned char[length]);
    HexDecodeToBinary(buf.get(), value.c_str());

    const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
    const int32_t minLength = length + lengthLength;
    Varlen *sref = Varlen::Create(minLength, data_pool);
    char *storage = sref->Get();
    SetObjectLengthToLocation(length, storage);
    ::memcpy(storage + lengthLength, buf.get(), length);
    retval.SetObjectValue(sref);
    retval.SetObjectLength(length);
    retval.SetObjectLengthLength(lengthLength);
    return retval;
  }

  static Value GetBinaryValue(const unsigned char *value, const int32_t length,
                              Pool *data_pool) {
    Value retval(VALUE_TYPE_VARBINARY);
    const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
    const int32_t minLength = length + lengthLength;
    Varlen *sref = Varlen::Create(minLength, data_pool);
    char *storage = sref->Get();
    SetObjectLengthToLocation(length, storage);
    ::memcpy(storage + lengthLength, value, length);
    retval.SetObjectValue(sref);
    retval.SetObjectLength(length);
    retval.SetObjectLengthLength(lengthLength);
    return retval;
  }

  static Value GetNullStringValue() {
    Value retval(VALUE_TYPE_VARCHAR);
    *reinterpret_cast<char **>(retval.value_data) = NULL;
    return retval;
  }

  static Value GetNullBinaryValue() {
    Value retval(VALUE_TYPE_VARBINARY);
    *reinterpret_cast<char **>(retval.value_data) = NULL;
    return retval;
  }

  static Value GetNullValue() {
    Value retval(VALUE_TYPE_NULL);
    return retval;
  }

  static Value GetDecimalValue(TTInt value) {
    Value retval(VALUE_TYPE_DECIMAL);
    retval.GetDecimal() = value;
    return retval;
  }

  static Value GetAddressValue(void *address) {
    Value retval(VALUE_TYPE_ADDRESS);
    *reinterpret_cast<void **>(retval.value_data) = address;
    return retval;
  }

  /**
   * @brief Do a deep copy of the given value.
   * Uninlined data will be allocated in the provided memory pool.
   */
  static Value Clone(const Value &src, Pool *dataPool = nullptr) {
    Value rv = src;  // Shallow copy first
    auto value_type = src.GetValueType();

    switch (value_type) {
      case VALUE_TYPE_VARBINARY:
      case VALUE_TYPE_VARCHAR:
        break;  // "real" deep copy is needed only for these types

      default:
        return rv;
    }

    if (src.is_inlined || src.IsNull()) {
      return rv;  // also, shallow copy for inlined or null data
    }

    Varlen *src_sref = *reinterpret_cast<Varlen *const *>(src.value_data);
    Varlen *new_sref = Varlen::Clone(*src_sref, dataPool);

    rv.SetObjectValue(new_sref);
    return rv;
  }

  /**
   * Copy the arbitrary size object that this value points to as an
   * inline object in the provided storage area
   */
  void InlineCopyObject(void *storage, int32_t max_length) const {
    if (IsNull()) {
      /*
       * The 7th bit of the length preceding value
       * is used to indicate that the object is null.
       */
      *reinterpret_cast<char *>(storage) = OBJECT_NULL_BIT;
    } else {
      const int32_t objectLength = GetObjectLength();
      if (objectLength > max_length) {
        char msg[1024];
        ::snprintf(msg, 1024,
                   "Object exceeds specified size. Size is %d and max is %d",
                   objectLength, max_length);
        throw ObjectSizeException(msg);
      }
      if (is_inlined) {
        ::memcpy(storage, *reinterpret_cast<char *const *>(value_data),
                 GetObjectLengthLength() + objectLength);
      } else {
        const Varlen *sref = *reinterpret_cast<Varlen *const *>(value_data);
        ::memcpy(storage, sref->Get(), GetObjectLengthLength() + objectLength);
      }
    }
  }
};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

/**
 * Public constructor that initializes to an Value that is unusable
 * with other Values.  Useful for declaring storage for an Value.
 */
inline Value::Value() {
  ::memset(value_data, 0, 16);
  SetValueType(VALUE_TYPE_INVALID);
}

/**
 * Objects may have storage allocated for them. Calling free causes the Value to
 * return the storage allocated for the object to the heap
 */
inline void Value::FreeUninlinedData() const {
  switch (GetValueType()) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      assert(!is_inlined);
      Varlen *sref = *reinterpret_cast<Varlen *const *>(value_data);
      if (sref != NULL) {
        Varlen::Destroy(sref);
      }
    } break;
    default:
      return;
  }
}

inline void *Value::CastAsAddress() const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_ADDRESS:
      return *reinterpret_cast<void *const *>(value_data);
    default:
      throw UnknownTypeException(
          (int)type, "Type %d not a recognized type for casting as an address");
  }
}

//===--------------------------------------------------------------------===//
// NULLs and BOOLs
//===--------------------------------------------------------------------===//

/**
 * Retrieve a boolean Value that is true
 */
inline Value Value::GetTrue() {
  Value retval(VALUE_TYPE_BOOLEAN);
  retval.GetBoolean() = true;
  return retval;
}

/**
 * Retrieve a boolean Value that is false
 */
inline Value Value::GetFalse() {
  Value retval(VALUE_TYPE_BOOLEAN);
  retval.GetBoolean() = false;
  return retval;
}

/**
 * Return a new Value that is the Opposite of this one. Only works on
 * booleans
 */
inline Value Value::OpNegate() const {
  assert(GetValueType() == VALUE_TYPE_BOOLEAN);
  Value retval(VALUE_TYPE_BOOLEAN);
  retval.GetBoolean() = !GetBoolean();
  return retval;
}

/**
 * Returns C++ true if this Value is a boolean and is true
 */
inline bool Value::IsTrue() const {
  assert(GetValueType() == VALUE_TYPE_BOOLEAN);
  return GetBoolean();
}

/**
 * Returns C++ false if this Value is a boolean and is true
 */
inline bool Value::IsFalse() const {
  assert(GetValueType() == VALUE_TYPE_BOOLEAN);
  return !GetBoolean();
}

inline bool Value::IsNegative() const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TINYINT:
      return GetTinyInt() < 0;
    case VALUE_TYPE_SMALLINT:
      return GetSmallInt() < 0;
    case VALUE_TYPE_INTEGER:
      return GetInteger() < 0;
    case VALUE_TYPE_BIGINT:
      return GetBigInt() < 0;
    case VALUE_TYPE_TIMESTAMP:
      return GetTimestamp() < 0;
    case VALUE_TYPE_DOUBLE:
      return GetDouble() < 0;
    case VALUE_TYPE_DECIMAL:
      return GetDecimal().IsSign();
    default: {
      throw UnknownTypeException(type,
                                 "Invalid value type for checking negativity");
    }
  }
}

/**
 * Logical and Operation for Values
 */
inline Value Value::OpAnd(const Value rhs) const {
  if (GetBoolean() && rhs.GetBoolean()) {
    return GetTrue();
  }
  return GetFalse();
}

/*
 * Logical or Operation for Values
 */
inline Value Value::OpOr(const Value rhs) const {
  if (GetBoolean() || rhs.GetBoolean()) {
    return GetTrue();
  }
  return GetFalse();
}

inline bool Value::IsNull() const {
  switch (GetValueType()) {
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_INVALID:
      return true;
    case VALUE_TYPE_TINYINT:
      return GetTinyInt() == INT8_NULL;
    case VALUE_TYPE_SMALLINT:
      return GetSmallInt() == INT16_NULL;
    case VALUE_TYPE_INTEGER:
      return GetInteger() == INT32_NULL;
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_BIGINT:
      return GetBigInt() == INT64_NULL;
    case VALUE_TYPE_ADDRESS:
      return *reinterpret_cast<void *const *>(value_data) == NULL;
    case VALUE_TYPE_DOUBLE:
      return GetDouble() <= DOUBLE_NULL;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      return (*reinterpret_cast<void *const *>(value_data) == nullptr ||
              *reinterpret_cast<const int32_t *>(&value_data[8]) ==
                  OBJECTLENGTH_NULL);
    case VALUE_TYPE_DECIMAL: {
      TTInt min;
      min.SetMin();
      return GetDecimal() == min;
    }
    default:
      throw IncompatibleTypeException(
          GetValueType(), "Value::isNull() called with ValueType '%d'");
  }
  return false;
}

/**
 * Set this Value to null.
 */
inline void Value::SetNull() {
  switch (GetValueType()) {
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_INVALID:
      return;
    case VALUE_TYPE_TINYINT:
      GetTinyInt() = INT8_NULL;
      break;
    case VALUE_TYPE_SMALLINT:
      GetSmallInt() = INT16_NULL;
      break;
    case VALUE_TYPE_INTEGER:
      GetInteger() = INT32_NULL;
      break;
    case VALUE_TYPE_TIMESTAMP:
      GetTimestamp() = INT64_NULL;
      break;
    case VALUE_TYPE_BIGINT:
      GetBigInt() = INT64_NULL;
      break;
    case VALUE_TYPE_DOUBLE:
      GetDouble() = DOUBLE_MIN;
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      *reinterpret_cast<void **>(value_data) = NULL;
      break;
    case VALUE_TYPE_DECIMAL:
      GetDecimal().SetMin();
      break;
    default: {
      throw IncompatibleTypeException(GetValueType(), "ValueType '%d'");
    }
  }
}

/**
 * Get the amount of storage necessary to store a value of the specified type
 * in a tuple
 */
inline uint16_t Value::GetTupleStorageSize(const ValueType type) {
  switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return sizeof(int64_t);
    case VALUE_TYPE_TINYINT:
      return sizeof(int8_t);
    case VALUE_TYPE_SMALLINT:
      return sizeof(int16_t);
    case VALUE_TYPE_INTEGER:
      return sizeof(int32_t);
    case VALUE_TYPE_DOUBLE:
      return sizeof(double);
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      return sizeof(char *);
    case VALUE_TYPE_DECIMAL:
      return sizeof(TTInt);
    default:
      char message[128];
      ::snprintf(message, 128, "unrecognized type %d", type);
      throw Exception(message);
  }
}

/**
 * Compare any two Values. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of Op*.
 */
inline int Value::Compare(const Value rhs) const {
  switch (GetValueType()) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
      if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
        return CastAsDouble().CompareDoubleValue(rhs);
      } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
        return -1 * rhs.CompareDecimalValue(*this);
      } else {
        return CompareAnyIntegerValue(rhs);
      }
    case VALUE_TYPE_TIMESTAMP:
      if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
        return CastAsDouble().CompareDoubleValue(rhs);
      } else {
        return CompareAnyIntegerValue(rhs);
      }
    case VALUE_TYPE_DOUBLE:
      return CompareDoubleValue(rhs);
    case VALUE_TYPE_VARCHAR:
      return CompareStringValue(rhs);
    case VALUE_TYPE_DECIMAL:
      return CompareDecimalValue(rhs);
    default: {
      throw IncompatibleTypeException(rhs.GetValueType(),
                                      "non comparable type '%d'");
    }
  }
}

//===--------------------------------------------------------------------===//
// Serialization/Deserialization utilities
//===--------------------------------------------------------------------===//

/**
 * Serialize the scalar this Value represents to the storage area
 * provided. If the scalar is an Object type then the object will
 * be copy if it can be inlined into the tuple. Otherwise a
 * pointer to the object will be copied into the storage area. No
 * allocations are performed.
 */
inline void Value::Serialize(void *storage, const bool return_is_inlined,
                             const int32_t max_length) const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t *>(storage) = GetTimestamp();
      break;
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t *>(storage) = GetTinyInt();
      break;
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t *>(storage) = GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t *>(storage) = GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
      *reinterpret_cast<int64_t *>(storage) = GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double *>(storage) = GetDouble();
      break;
    case VALUE_TYPE_DECIMAL:
      ::memcpy(storage, value_data, Value::GetTupleStorageSize(type));
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      // Potentially non-inlined type requires special handling
      if (return_is_inlined) {
        InlineCopyObject(storage, max_length);
      } else {
        if (IsNull() || GetObjectLength() <= max_length) {
          if (is_inlined && !return_is_inlined) {
            throw Exception(
                "Cannot serialize an inlined string to non-inlined tuple "
                "storage in serializeToTupleStorage()");
          }
          // copy the StringRef pointers
          *reinterpret_cast<Varlen **>(storage) =
              *reinterpret_cast<Varlen *const *>(value_data);
        } else {
          const int32_t length = GetObjectLength();
          char msg[1024];
          ::snprintf(msg, 1024,
                     "Object exceeds specified size. Size is %d and max is %d",
                     length, max_length);
          throw ObjectSizeException(msg);
        }
      }
      break;
    default:
      char message[128];
      ::snprintf(message, 128,
                 "Value::serializeToTupleStorage() unrecognized type '%d'",
                 type);
      throw ObjectSizeException(message);
  }
}

/**
 * Serialize this Value to the provided SerializeOutput
 */
inline void Value::SerializeTo(SerializeOutput &output) const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      if (IsNull()) {
        output.WriteInt(OBJECTLENGTH_NULL);
        break;
      }
      const int32_t length = GetObjectLength();
      if (length < OBJECTLENGTH_NULL) {
        throw Exception(
            "Attempted to serialize an Value with a negative length");
      }
      output.WriteInt(static_cast<int32_t>(length));
      if (length != OBJECTLENGTH_NULL) {
        // Not a null string: write it out
        const char *str = reinterpret_cast<const char *>(GetObjectValue());
        if (str == NULL) {
        }
        output.WriteBytes(GetObjectValue(), length);
      } else {
        assert(GetObjectValue() == NULL || length == OBJECTLENGTH_NULL);
      }

      break;
    }
    case VALUE_TYPE_TINYINT: {
      output.WriteByte(GetTinyInt());
      break;
    }
    case VALUE_TYPE_SMALLINT: {
      output.WriteShort(GetSmallInt());
      break;
    }
    case VALUE_TYPE_INTEGER: {
      output.WriteInt(GetInteger());
      break;
    }
    case VALUE_TYPE_TIMESTAMP: {
      output.WriteLong(GetTimestamp());
      break;
    }
    case VALUE_TYPE_BIGINT: {
      output.WriteLong(GetBigInt());
      break;
    }
    case VALUE_TYPE_DOUBLE: {
      output.WriteDouble(GetDouble());
      break;
    }
    case VALUE_TYPE_DECIMAL: {
      output.WriteLong(GetDecimal().table[1]);
      output.WriteLong(GetDecimal().table[0]);
      break;
    }
    default:
      throw UnknownTypeException(type,
                                 "Value::serializeTo() found a column "
                                 "with ValueType '%d' that is not handled");
  }
}

inline void Value::SerializeToExport(ExportSerializeOutput &io) const {
  switch (GetValueType()) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP: {
      int64_t val = CastAsBigIntAndGetValue();
      io.WriteLong(val);
      return;
    }
    case VALUE_TYPE_DOUBLE: {
      double value = GetDouble();
      io.WriteDouble(value);
      return;
    }
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      // requires (and uses) bytecount not character count
      io.WriteBinaryString(GetObjectValue(), GetObjectLength());
      return;
    }
    case VALUE_TYPE_DECIMAL: {
      std::string decstr = CreateStringFromDecimal();
      int32_t objectLength = (int32_t)decstr.length();
      io.WriteBinaryString(decstr.data(), objectLength);
      return;
    }
    case VALUE_TYPE_INVALID:
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_BOOLEAN:
    case VALUE_TYPE_ADDRESS:
      char message[128];
      ::snprintf(message, 128, "Invalid type in serializeToExport: %d",
                 GetValueType());
      throw UnknownTypeException(GetValueType(), message);
  }

  throw UnknownTypeException(GetValueType(), "");
}

/**
 * Serialize the scalar this Value represents to the provided
 * storage area. If the scalar is an Object type that is not
 * inlined then the provided data pool or the heap will be used to
 * allocated storage for a copy of the object.
 */
inline void Value::SerializeWithAllocation(void *storage, const bool is_inlined,
                                           const int32_t max_length,
                                           Pool *data_pool) const {
  const ValueType type = GetValueType();
  int32_t length = 0;

  switch (type) {
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t *>(storage) = GetTimestamp();
      break;
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t *>(storage) = GetTinyInt();
      break;
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t *>(storage) = GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t *>(storage) = GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
      *reinterpret_cast<int64_t *>(storage) = GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double *>(storage) = GetDouble();
      break;
    case VALUE_TYPE_DECIMAL:
      ::memcpy(storage, value_data, Value::GetTupleStorageSize(type));
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      // Potentially non-inlined type requires special handling
      if (is_inlined) {
        InlineCopyObject(storage, max_length);
      } else {
        if (IsNull()) {
          *reinterpret_cast<void **>(storage) = NULL;
        } else {
          length = GetObjectLength();
          const int8_t lengthLength = GetObjectLengthLength();
          const int32_t minlength = lengthLength + length;
          if (length > max_length) {
            char msg[1024];
            ::snprintf(msg, 1024,
                       "Object exceeds specified size. Size is %d"
                       " and max is %d",
                       length, max_length);
            throw ObjectSizeException(msg);
          }

          Varlen *sref = Varlen::Create(minlength, data_pool);
          char *copy = sref->Get();
          SetObjectLengthToLocation(length, copy);
          ::memcpy(copy + lengthLength, GetObjectValue(), length);
          *reinterpret_cast<Varlen **>(storage) = sref;
        }
      }
      break;
    default: { throw UnknownTypeException(type, "unrecognized type '%d'"); }
  }
}

/**
 * Deserialize a scalar of the specified type from the tuple
 * storage area provided. If this is an Object type then the third
 * argument indicates whether the object is stored in the tuple
 * inline
 *
 * TODO: Could the is_inlined argument be removed by have the
 * caller dereference the pointer?
 */
inline const Value Value::Deserialize(const void *storage, const ValueType type,
                                      const bool is_inlined) {
  Value retval(type);
  switch (type) {
    case VALUE_TYPE_TIMESTAMP:
      retval.GetTimestamp() = *reinterpret_cast<const int64_t *>(storage);
      break;
    case VALUE_TYPE_TINYINT:
      retval.GetTinyInt() = *reinterpret_cast<const int8_t *>(storage);
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetSmallInt() = *reinterpret_cast<const int16_t *>(storage);
      break;
    case VALUE_TYPE_INTEGER:
      retval.GetInteger() = *reinterpret_cast<const int32_t *>(storage);
      break;
    case VALUE_TYPE_BIGINT:
      retval.GetBigInt() = *reinterpret_cast<const int64_t *>(storage);
      break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = *reinterpret_cast<const double *>(storage);
      break;
    case VALUE_TYPE_DECIMAL:
      ::memcpy(retval.value_data, storage, Value::GetTupleStorageSize(type));
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      // Potentially non-inlined type requires special handling
      char *data = NULL;

      if (is_inlined) {
        // If it is inlined the storage area contains the actual data so copy a
        // reference
        // to the storage area
        *reinterpret_cast<void **>(retval.value_data) =
            const_cast<void *>(storage);
        data = *reinterpret_cast<char **>(retval.value_data);
        retval.SetSourceInlined(true);
      } else {
        // If it isn't inlined the storage area contains a pointer to the
        // StringRef object containing the string's memory
        memcpy(retval.value_data, storage, sizeof(void *));
        Varlen *sref = nullptr;
        sref = *reinterpret_cast<Varlen **>(retval.value_data);

        // If the StringRef pointer is null, that's because this
        // was a null value; leave the data pointer as NULL so
        // that GetObjectLengthFromLocation will figure this out
        // correctly, otherwise Get the right char* from the StringRef
        if (sref != NULL) {
          data = sref->Get();
        }
      }

      const int32_t length = GetObjectLengthFromLocation(data);
      // std::cout << "Value::deserializeFromTupleStorage: length: " << length
      // << std::endl;
      retval.SetObjectLength(length);
      retval.SetObjectLengthLength(GetAppropriateObjectLengthLength(length));
      break;
    }
    default:
      throw IncompatibleTypeException(type, "unrecognized type");
  }
  return retval;
}

/**
 * Deserialize a scalar value of the specified type from the
 * SerializeInput directly into the tuple storage area
 * provided. This function will perform memory allocations for
 * Object types as necessary using the provided data pool or the
 * heap. This is used to deserialize tables.
 */
inline int64_t Value::DeserializeFrom(SerializeInput &input,
                                      const ValueType type, char *storage,
                                      bool is_inlined, const int32_t max_length,
                                      Pool *data_pool) {
  switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t *>(storage) = input.ReadLong();
      return sizeof(int64_t);
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t *>(storage) = input.ReadByte();
      return sizeof(int8_t);
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t *>(storage) = input.ReadShort();
      return sizeof(int16_t);
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t *>(storage) = input.ReadInt();
      return sizeof(int32_t);
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double *>(storage) = input.ReadDouble();
      return sizeof(double);
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      int64_t bytesRead = 0;
      const int32_t length = input.ReadInt();
      bytesRead += sizeof(int32_t);
      if (length > max_length) {
        char msg[1024];
        ::snprintf(msg, 1024,
                   "String exceeds specified size. Size is %d and max is %d",
                   length, max_length);
        throw ObjectSizeException(msg);
      }

      const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
      // the NULL SQL string is a NULL C pointer
      if (is_inlined) {
        SetObjectLengthToLocation(length, storage);
        if (length == OBJECTLENGTH_NULL) {
          return 0;
        }
        const char *data =
            reinterpret_cast<const char *>(input.GetRawPointer(length));
        ::memcpy(storage + lengthLength, data, length);
      } else {
        if (length == OBJECTLENGTH_NULL) {
          *reinterpret_cast<void **>(storage) = NULL;
          return 0;
        }
        const char *data =
            reinterpret_cast<const char *>(input.GetRawPointer(length));
        const int32_t minlength = lengthLength + length;
        Varlen *sref = Varlen::Create(minlength, data_pool);
        char *copy = sref->Get();
        SetObjectLengthToLocation(length, copy);
        ::memcpy(copy + lengthLength, data, length);
        *reinterpret_cast<Varlen **>(storage) = sref;
      }
      bytesRead += length;
      return bytesRead;
    }
    case VALUE_TYPE_DECIMAL: {
      int64_t *longStorage = reinterpret_cast<int64_t *>(storage);
      // Reverse order for Java BigDecimal BigEndian
      longStorage[1] = input.ReadLong();
      longStorage[0] = input.ReadLong();
      return 2 * sizeof(long);
    }
    default:
      char message[128];
      ::snprintf(message, 128,
                 "Value::deserializeFrom() unrecognized type '%d'", type);
      throw UnknownTypeException(type, message);
  }
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter sets.
 */
inline const Value Value::DeserializeWithAllocation(SerializeInput &input,
                                                    Pool *data_pool) {
  const ValueType type = static_cast<ValueType>(input.ReadByte());
  Value retval(type);
  switch (type) {
    case VALUE_TYPE_BIGINT:
      retval.GetBigInt() = input.ReadLong();
      break;
    case VALUE_TYPE_TIMESTAMP:
      retval.GetTimestamp() = input.ReadLong();
      break;
    case VALUE_TYPE_TINYINT:
      retval.GetTinyInt() = input.ReadByte();
      break;
    case VALUE_TYPE_SMALLINT:
      retval.GetSmallInt() = input.ReadShort();
      break;
    case VALUE_TYPE_INTEGER:
      retval.GetInteger() = input.ReadInt();
      break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = input.ReadDouble();
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY: {
      const int32_t length = input.ReadInt();
      const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
      // the NULL SQL string is a NULL C pointer
      if (length == OBJECTLENGTH_NULL) {
        retval.SetNull();
        break;
      }
      const void *str = input.GetRawPointer(length);
      const int32_t minlength = lengthLength + length;
      Varlen *sref = Varlen::Create(minlength, data_pool);
      char *copy = sref->Get();
      retval.SetObjectLengthToLocation(length, copy);
      ::memcpy(copy + lengthLength, str, length);
      retval.SetObjectValue(sref);
      retval.SetObjectLength(length);
      retval.SetObjectLengthLength(lengthLength);
      break;
    }
    case VALUE_TYPE_DECIMAL: {
      retval.GetDecimal().table[1] = input.ReadLong();
      retval.GetDecimal().table[0] = input.ReadLong();
      break;
    }
    case VALUE_TYPE_NULL: {
      retval.SetNull();
      break;
    }
    default:
      throw UnknownTypeException(
          type,
          "Value::deserializeFromAllocateForStorage() unrecognized type '%d'");
  }
  return retval;
}

}  // End peloton namespace
