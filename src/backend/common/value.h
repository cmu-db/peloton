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
#include <cstdlib>
#include <algorithm>
#include <vector>
#include <iostream>

#include "backend/common/exception.h"
#include "backend/common/pool.h"
#include "backend/common/serializer.h"
#include "backend/common/types.h"
#include "backend/common/varlen.h"
#include "backend/common/logger.h"

#include "boost/scoped_ptr.hpp"
#include "boost/functional/hash.hpp"
#include "ttmath/ttmathint.h"
#include "utf8.h"
#include "murmur3/MurmurHash3.h"

namespace peloton {

//===--------------------------------------------------------------------===//
// Type system
//===--------------------------------------------------------------------===//

/*
 * Objects are length preceded with a short length value or a long length value
 * depending on how many bytes are needed to represent the length. These
 * define how many bytes are used for the short value vs. the long value.
 */
#define SHORT_OBJECT_LENGTHLENGTH static_cast<char>(1)
#define LONG_OBJECT_LENGTHLENGTH static_cast<char>(4)
#define OBJECT_NULL_BIT static_cast<char>(1 << 6)
#define OBJECT_CONTINUATION_BIT static_cast<char>(1 << 7)
#define OBJECT_MAX_LENGTH_SHORT_LENGTH 63

#define FULL_STRING_IN_MESSAGE_THRESHOLD 100

//The int used for storage and return values
typedef ttmath::Int<2> TTInt;
//Long integer with space for multiplication and division without carry/overflow
typedef ttmath::Int<4> TTLInt;

template<typename T>
void ThrowCastSQLValueOutOfRangeException(
    const T value,
    const ValueType origType,
    const ValueType newType)
{
  ThrowCastSQLValueOutOfRangeException((const int64_t)value, origType, newType);
}

template<>
inline void ThrowCastSQLValueOutOfRangeException<double>(
    const double value,
    const ValueType origType,
    const ValueType newType)
    {
  char msg[1024];
  snprintf(msg, 1024, "Type %s with value %f can't be cast as %s because the value is "
           "out of range for the destination type",
           ValueTypeToString(origType).c_str(),
           value,
           ValueTypeToString(newType).c_str());

  throw NumericValueOutOfRangeException(msg, 0);
    }

template<>
inline void ThrowCastSQLValueOutOfRangeException<int64_t>(
    const int64_t value,
    const ValueType origType,
    const ValueType newType)
    {
  char msg[1024];
  snprintf(msg, 1024, "Type %s with value %jd can't be cast as %s because the value is "
           "out of range for the destination type",
           ValueTypeToString(origType).c_str(),
           (intmax_t)value,
           ValueTypeToString(newType).c_str());

  // record underflow or overflow for executors that catch this (indexes, mostly)
  int internalFlags = 0;
  if (value > 0) internalFlags |= NumericValueOutOfRangeException::TYPE_OVERFLOW;
  if (value < 0) internalFlags |= NumericValueOutOfRangeException::TYPE_UNDERFLOW;

  throw NumericValueOutOfRangeException(msg, internalFlags);
    }

template<>
inline void ThrowCastSQLValueOutOfRangeException<TTInt>(
    const TTInt value,
    const ValueType origType,
    const ValueType newType)
    {
  char msg[1024];
  snprintf(msg, 1024, "Type %s with value %s can't be cast as %s because the value is "
           "out of range for the destination type",
           ValueTypeToString(origType).c_str(),
           value.ToString().c_str(),
           ValueTypeToString(newType).c_str());

  // record underflow or overflow for executors that catch this (indexes, mostly)
  int internalFlags = 0;
  if (value > 0) internalFlags |= NumericValueOutOfRangeException::TYPE_OVERFLOW;
  if (value < 0) internalFlags |= NumericValueOutOfRangeException::TYPE_UNDERFLOW;

  throw NumericValueOutOfRangeException(msg, internalFlags);
    }

int WarnIf(int condition, const char* message);

// This has been demonstrated to be more reliable than std::isinf
// -- less sensitive on LINUX to the "g++ -ffast-math" option.
inline int NonStdIsInf( double x ) { return (x > DBL_MAX) || (x < -DBL_MAX); }

inline void ThrowDataExceptionIfInfiniteOrNaN(double value, const char* function)
{
  static int warned_once_no_nan = WarnIf( ! std::isnan(sqrt(-1.0)),
                                           "The C++ configuration (e.g. \"g++ --fast-math\") "
                                           "does not support SQL standard handling of NaN errors.");
  static int warned_once_no_inf = WarnIf( ! NonStdIsInf(std::pow(0.0, -1.0)),
                                           "The C++ configuration (e.g. \"g++ --fast-math\") "
                                           "does not support SQL standard handling of numeric infinity errors.");
  // This uses a standard test for NaN, even though that fails in some configurations Like LINUX "g++ -ffast-math".
  // If it is known to fail in the current config, a warning has been sent to the log,
  // so at this point, just relax the check.
  if ((warned_once_no_nan || ! std::isnan(value)) && (warned_once_no_inf || ! NonStdIsInf(value))) {
    return;
  }
  char msg[1024];
  snprintf(msg, sizeof(msg), "Invalid result value (%f) from floating point %s", value, function);
  throw NumericValueOutOfRangeException(std::string(msg), 0);
}


/// Stream out a double value in SQL standard format, a specific variation of E-notation.
/// TODO: it has been suggested that helper routines Like this that are not specifiCally tied to
/// the Value representation should be defined in some other header to help reduce clutter, here.
inline void StreamSQLFloatFormat(std::stringstream& streamOut, double floatValue)
{
  // Standard SQL wants capital E scientific notation.
  // Yet it differs in some detail from C/C++ E notation, even with all of its customization options.

  // For starters, for 0, the standard explicitly Calls for '0E0'.
  // For across-the-board compatibility, the HSQL backend had to be patched it was using '0.0E0'.
  // C++ uses 0.000000E+00 by default. So override that explicitly.
  if (0.0 == floatValue) {
    streamOut << "0E0";
    return;
  }
  // For other values, C++ generally adds too much garnish to be standard
  // -- trailing zeros in the mantissa, an explicit '+' on the exponent, and a
  // leading 0 before single-digit exponents.  Trim it down to the minimalist sql standard.
  std::stringstream fancy;
  fancy << std::setiosflags(std::ios::scientific | std::ios::uppercase) << floatValue;
  // match any format with the regular expression:
  std::string fancyText = fancy.str();
  size_t ePos = fancyText.find('E', 3); // find E after "[-]n.n".
  assert(ePos != std::string::npos);
  size_t endSignifMantissa;
  // Never truncate mantissa down to the bare '.' EVEN for the case of "n.0".
  for (endSignifMantissa = ePos; fancyText[endSignifMantissa-2] != '.'; --endSignifMantissa) {
    // Only truncate trailing '0's.
    if (fancyText[endSignifMantissa-1] != '0') {
      break; // from loop
    }
  }
  const char* optionalSign = (fancyText[ePos+1] == '-') ? "-" : "";
  size_t startSignifExponent;
  // Always keep at least 1 exponent digit.
  size_t endExponent = fancyText.length()-1;
  for (startSignifExponent = ePos+1; startSignifExponent < endExponent; ++startSignifExponent) {
    const char& exponentLeadChar = fancyText[startSignifExponent];
    // Only skip leading '-'s, '+'s and '0's.
    if (exponentLeadChar != '-' && exponentLeadChar != '+' && exponentLeadChar != '0') {
      break; // from loop
    }
  }
  // Bring the truncated pieces toGether.
  streamOut << fancyText.substr(0, endSignifMantissa)
                                          << 'E' << optionalSign << fancyText.substr(startSignifExponent);
}

/**
 * A class to wrap all scalar values regardless of type and
 * storage. An Value is not the representation used in the
 * serialization of VoltTables nor is it the representation of how
 * scalar values are stored in tables. Value does have serialization
 * and deserialization mechanisms for both those storage formats.
 * Values are designed to be immutable and for the most part not
 * constructable from raw data types. Access to the raw data is
 * restricted so that all operations have to go through the member
 * functions that can perform the correct casting and error
 * checking. ValueFactory can be used to construct new Values, but
 * that should be avoided if possible.
 */
class Value {
  friend class ValuePeeker;
  friend class ValueFactory;

 public:
  /* Create a default Value */
  Value();

  /* Release memory associated to object type Values */
  ~Value();

  Value& operator= (const Value & other);

  Value(const Value& other);

  /* Release memory associated to object type Values */
  void Free() const;

  /* Release memory associated to object type tuple columns */
  static void FreeObjectsFromTupleStorage(std::vector<char*> const &oldObjects);

  /* Set value to the correct SQL NULL representation. */
  void SetNull();

  /* Reveal the contained pointer for type values  */
  void* CastAsAddress() const;

  /* Create a boolean true Value */
  static Value GetTrue();

  /* Create a boolean false Value */
  static Value GetFalse();

  /* Create an Value with the null representation for valueType */
  static Value GetNullValue(ValueType);

  /* Create an Value promoted/demoted to type */
  Value CastAs(ValueType type) const;

  // todo: Why doesn't this return size_t? Also, this is a
  // quality of ValueType, not Value.

  /* Calculate the tuple storage size for an Value type. VARCHARs
       assume out-of-band tuple storage */
  static uint16_t GetTupleStorageSize(const ValueType type);

  // todo: Could the isInlined argument be removed by have the
  // Caller dereference the pointer?

  /* Deserialize a scalar of the specified type from the tuple
       storage area provided. If this is an Object type then the third
       argument indicates whether the object is stored in the tuple
       inline */
  static Value InitFromTupleStorage(const void *storage, ValueType type, bool isInlined);

  /* Serialize the scalar this Value represents to the provided
       storage area. If the scalar is an Object type that is not
       inlined then the provided data pool or the heap will be used to
       allocated storage for a copy of the object. */
  void SerializeToTupleStorageAllocateForObjects(
      void *storage, const bool isInlined, const int32_t maxLength,
      const bool isInBytes, VarlenPool *dataPool) const;

  /* Serialize the scalar this Value represents to the storage area
       provided. If the scalar is an Object type then the object will
       be copy if it can be inlined into the tuple. Otherwise a
       pointer to the object will be copied into the storage area. Any
       allocations needed (if this Value refers to inlined memory
       whereas the field in the tuple is not inlined), will be done in
       the temp string pool. */
  void SerializeToTupleStorage(
      void *storage, const bool isInlined, const int32_t maxLength, const bool isInBytes) const;

  /* Deserialize a scalar value of the specified type from the
       SerializeInput directly into the tuple storage area
       provided. This function will perform memory allocations for
       Object types as necessary using the provided data pool or the
       heap. This is used to deserialize tables. */
  template <TupleSerializationFormat F, Endianess E>
  static void DeserializeFrom(
      SerializeInput<E> &input, VarlenPool *dataPool, char *storage,
      const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes);
  static void DeserializeFrom(
      SerializeInputBE &input, VarlenPool *dataVarlenPool, char *storage,
      const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes);

  // TODO: no Callers use the first form; Should combine these
  // eliminate the potential Value copy.

  /* Read a ValueType from the SerializeInput stream and deserialize
       a scalar value of the specified type into this Value from the provided
       SerializeInput and perform allocations as necessary. */
  void DeserializeFromAllocateForStorage(SerializeInputBE &input, VarlenPool *dataPool);
  void DeserializeFromAllocateForStorage(ValueType vt, SerializeInputBE &input, VarlenPool *dataPool);

  /* Serialize this Value to a SerializeOutput */
  void SerializeTo(SerializeOutput &output) const;

  /* Serialize this Value to an Export stream */
  void SerializeToExportWithoutNull(ExportSerializeOutput&) const;

  // See comment with inlined body, below.  If NULL is supplied for
  // the pool, use the temp string pool.
  void AllocateObjectFromInlinedValue(VarlenPool* pool);

  void AllocateObjectFromOutlinedValue();

  /* Check if the value represents SQL NULL */
  bool IsNull() const;

  /* Check if the value represents IEEE 754 NaN */
  bool IsNan() const;

  /* For boolean Values, convert to bool */
  bool IsTrue() const;
  bool IsFalse() const;

  /* Tell Caller if this Value's value refers back to VARCHAR or
       VARBINARY data internal to a TableTuple (and not a
       Varlen) */
  bool GetSourceInlined() const;

  /* For number values, check the number line. */
  bool IsZero() const;

  /* For boolean Values only, logical operators */
  Value Opnegate() const;
  Value Opand(const Value rhs) const;
  Value Opor(const Value rhs) const;

  /* Evaluate the ordering relation against two Values. Promotes
       exact types to allow disparate type comparison. See also the
       Op functions which return boolean Values.
   */
  int CompareNull(const Value rhs) const;
  int Compare(const Value rhs) const;
  int CompareWithoutNull(const Value rhs) const;

  ////////////////////////////////////////////////////////////
  // TODO: Peloton Changes
  ////////////////////////////////////////////////////////////

  inline bool operator==(const Value &other) const {
     if (this->Compare(other) == 0) {
       return true;
     }

     return false;
  }

  inline bool operator!=(const Value &other) const { return !(*this == other); }

  /**
   * @brief Do a deep copy of the given value.
   * Uninlined data will be allocated in the provided memory pool.
   */
  static Value Clone(const Value &src, VarlenPool *dataPool);

  // Get min value
  static Value GetMinValue(ValueType);

  int GetIntegerForTestsOnly() { return GetInteger(); }

  ////////////////////////////////////////////////////////////

  /* Return a boolean Value with the comparison result */
  Value OpEquals(const Value rhs) const;
  Value OpNotEquals(const Value rhs) const;
  Value OpLessThan(const Value rhs) const;
  Value OpLessThanOrEqual(const Value rhs) const;
  Value OpGreaterThan(const Value rhs) const;
  Value OpGreaterThanOrEqual(const Value rhs) const;

  Value OpEqualsWithoutNull(const Value rhs) const;
  Value OpNotEqualsWithoutNull(const Value rhs) const;
  Value OpLessThanWithoutNull(const Value rhs) const;
  Value OpLessThanOrEqualWithoutNull(const Value rhs) const;
  Value OpGreaterThanWithoutNull(const Value rhs) const;
  Value OpGreaterThanOrEqualWithoutNull(const Value rhs) const;


  /* Return a copy of MAX(this, rhs) */
  Value OpMax(const Value rhs) const;

  /* Return a copy of MIN(this, rhs) */
  Value OpMin(const Value rhs) const;

  /* For number Values, compute new Values for arithmetic operators */
  Value OpIncrement() const;
  Value OpDecrement() const;
  Value OpSubtract(const Value rhs) const;
  Value OpAdd(const Value rhs) const;
  Value OpMultiply(const Value rhs) const;
  Value OpDivide(const Value rhs) const;
  /*
   * This Value must be VARCHAR and the rhs must be VARCHAR.
   * This Value is the value and the rhs is the pattern
   */
  Value Like(const Value rhs) const;

  //TODO: passing Value arguments by const reference SHOULD be standard practice
  // for the dozens of Value "operator" functions. It saves on needless Value copies.
  //TODO: returning bool (vs. Value GetTrue()/GetFalse()) SHOULD be standard practice
  // for Value "logical operator" functions.
  // It saves on needless Value copies and makes unit tests more readable.
  // Cases that need the Value -- for some actual purpose other than an immediate Call to
  // "IsTrue()" -- are rare and Getting rarer as optimizations Like short-cut eval are introduced.
  /**
   * Return true if this Value is listed as a member of the IN LIST
   * represented as an ValueList* value cached in rhsList.
   */
  bool InList(Value const& rhsList) const;

  /**
   * If this Value is an array value, Get it's length.
   * Undefined behavior if not an array (cassert fail in Debug).
   */
  int ArrayLength() const;

  /**
   * If this Value is an array value, Get a value.
   * Undefined behavior if not an array or if oob (cassert fail in Debug).
   */
  Value ItemAtIndex(int index) const;

  /**
   * Used for SQL-IN-LIST to cast all array values to a specific type,
   * then sort an dedup them. Returns in a parameter vector, mostly for memory
   * management reasons. Dedup is important for index-accelerated plans, as
   * they might return duplicate rows from the inner join.
   * See MaterializedScanPlanNode & MaterializedScanExecutor
   *
   * Undefined behavior if not an array (cassert fail in Debug).
   */
  void CastAndSortAndDedupArrayForInList(const ValueType outputType, std::vector<Value> &outList) const;

  /*
   * Out must have space for 16 bytes
   */
  int32_t MurmurHash3() const;

  /*
   * CallConstant, CallUnary, and Call are templates for arbitrary Value member functions that implement
   * SQL "column functions". They differ in how many arguments they accept:
   * 0 for CallConstant, 1 ("this") for CallUnary, and any number (packaged in a vector) for Call.
   * The main benefit of these functions being (always explicit) template instantiations for each
   * "FUNC_*" int value instead of a more normal named member function
   * of Value is that it allows them to be invoked from the default eval method of the
   * correspondingly templated expression subclass
   * (ConstantFunctionExpression, UnaryFunctionExpression, or GeneralFunctionExpression).
   * The alternative would be to name each function (abs, substring, etc.)
   * and explicitly implement the eval method for every expression subclass template instantiation
   * (UnaryFunctionExpression<FUNC_ABS>::eval,
   * GeneralFunctionExpression<FUNC_LOG_SUBSTRING_FROM>::eval, etc.
   * to Call the corresponding Value member function.
   * So, these member function templates save a bit of boilerplate for each SQL function and allow
   * the function expression subclass templates to be implemented completely generiCally.
   */
  template <int F> // template for SQL functions returning constants (Like pi)
  static Value CallConstant();

  template <int F> // template for SQL functions of one Value ("this")
  Value CallUnary() const;

  template <int F> // template for SQL functions of multiple Values
  static Value Call(const std::vector<Value>& arguments);

  /// Iterates over UTF8 strings one character "code point" at a time, being careful not to walk off the end.
  class UTF8Iterator {
   public:
    UTF8Iterator(const char *start, const char *end) :
      m_cursor(start),
      m_end(end)
   // TODO: We could validate up front that the string is well-formed UTF8,
   // at least to the extent that multi-byte characters have a valid
   // prefix byte and continuation bytes that will not cause a read
   // off the end of the buffer.
   // That done, ExtractCodePoint could be considerably simpler/faster.
   { assert(m_cursor <= m_end); }

    //Construct a one-off with an alternative current cursor position
    UTF8Iterator(const UTF8Iterator& other, const char *start) :
      m_cursor(start),
      m_end(other.m_end)
    { assert(m_cursor <= m_end); }

    const char * GetCursor() { return m_cursor; }

    bool AtEnd() { return m_cursor >= m_end; }

    const char * SkipCodePoints(int64_t skips) {
      while (skips-- > 0 && ! AtEnd()) {
        // TODO: since the returned code point is ignored, it might be better
        // to Call a faster, simpler, skipCodePoint method -- maybe once that
        // becomes trivial due to up-front validation.
        ExtractCodePoint();
      }
      if (AtEnd()) {
        return m_end;
      }
      return m_cursor;
    }

    /*
     * Go through a lot of trouble to make sure that corrupt
     * utf8 data doesn't result in touching uninitialized memory
     * by copying the character data onto the stack.
     * That wouldn't be needed if we pre-validated the buffer.
     */
    uint32_t ExtractCodePoint() {
      assert(m_cursor < m_end); // Caller should have tested and handled AtEnd() condition
      /*
       * Copy the next 6 bytes to a temp buffer and retrieve.
       * We should only Get 4 byte code points, and the library
       * should only accept 4 byte code points, but once upon a time there
       * were 6 byte code points in UTF-8 so be careful here.
       */
      char nextPotentialCodePoint[] = { 0, 0, 0, 0, 0, 0 };
      char *nextPotentialCodePointIter = nextPotentialCodePoint;
      //Copy 6 bytes or until the end
      ::memcpy( nextPotentialCodePoint, m_cursor, std::min( 6L, m_end - m_cursor));

      /*
       * Extract the code point, find out how many bytes it was
       */
      uint32_t codePoint = utf8::unchecked::next(nextPotentialCodePointIter);
      long int delta = nextPotentialCodePointIter - nextPotentialCodePoint;

      /*
       * Increment the iterator that was passed in by ref, by the delta
       */
      m_cursor += delta;
      return codePoint;
    }

    const char * m_cursor;
    const char * const m_end;
  };


  /* For boost hashing */
  void HashCombine(std::size_t &seed) const;

  // Get a string representation of this value
  friend std::ostream &operator<<(std::ostream &os, const Value &value);

  /* Functor comparator for use with std::Set */
  struct ltValue {
    bool operator()(const Value v1, const Value v2) const {
      return v1.Compare(v2) < 0;
    }
  };

  /* Functor equality predicate for use with boost unordered */
  struct equal_to : std::binary_function<Value, Value, bool>
  {
    bool operator()(Value const& x,
                    Value const& y) const
    {
      return x.Compare(y) == 0;
    }
  };

  /* Functor hash predicate for use with boost unordered */
  struct hash : std::unary_function<Value, std::size_t>
  {
    std::size_t operator()(Value const& x) const
    {
      std::size_t seed = 0;
      x.HashCombine(seed);
      return seed;
    }
  };

  /* Return a string full of arcana and wonder. */
  std::string Debug() const;

  // Constants for Decimal type
  // Precision and scale (inherent in the schema)
  static const uint16_t kMaxDecPrec = 38;
  static const uint16_t kMaxDecScale = 12;
  static const int64_t kMaxScaleFactor = 1000000000000;         // == 10**12
 private:
  // Our maximum scale is 12.  Our maximum precision is 38.  So,
  // the maximum number of decimal digits is 38 - 12 = 26.  We can't
  // represent 10**26 in a 64 bit integer, but we can represent 10**18.
  // So, to test if a TTInt named m is too big we test if
  // m/kMaxWholeDivisor < kMaxWholeFactor
  static const uint64_t kMaxWholeDivisor = 100000000;             // == 10**8
  static const uint64_t kMaxWholeFactor = 1000000000000000000;    // == 10**18
  static bool inline oversizeWholeDecimal(TTInt ii) {
    return (TTInt(kMaxWholeFactor) <= ii / kMaxWholeDivisor);
  }
 public:
  // SetArrayElements is a const method since it doesn't actually mutate any Value state, just
  // the state of the contained Values which are referenced via the allocated object storage.
  // For example, it is not intended to ever "grow the array" which would require the Value's
  // object reference (in m_data) to be mutable.
  // The array size is predetermined in AllocateANewValueList.
  void SetArrayElements(std::vector<Value> &args) const;

  static ValueType PromoteForOp(ValueType vta, ValueType vtb) {
    ValueType rt;
    switch (vta) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP:
        rt = s_intPromotionTable[vtb];
        break;

      case VALUE_TYPE_DECIMAL:
        rt = s_decimalPromotionTable[vtb];
        break;

      case VALUE_TYPE_DOUBLE:
        rt = s_doublePromotionTable[vtb];
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
    // There ARE rare but legitimate runtime type check exceptions in SQL, so
    // unless/until those legitimate cases Get re-routed to some other code path,
    // it is not safe here to ...
    // assert(rt != VALUE_TYPE_INVALID);
    return rt;
  }

  // Declared public for cppunit test purposes .
  static int64_t parseTimestampString(const std::string &txt);

  static inline int32_t GetCharLength(const char *valueChars, const size_t length) {
    // very efficient code to count characters in UTF string and ASCII string
    int32_t j = 0;
    size_t i = length;
    while (i-- > 0) {
      if ((valueChars[i] & 0xc0) != 0x80) j++;
    }
    return j;
  }

  static inline int32_t GetIthCharIndex(const char *valueChars, const int64_t length, const int64_t ith) {
    if (ith <= 0) return -1;
    int32_t i = 0, j = 0;

    while (i < length) {
      if ((valueChars[i] & 0xc0) != 0x80) {
        if (++j == ith) break;
      }
      i++;
    }
    return i;
  }

  // Return the beginning char * place of the ith char.
  // Return the end char* when ith is larger than it has, NULL if ith is less and equal to zero.
  static inline const char* GetIthCharPosition(const char *valueChars, const size_t length, const int32_t ith) {
    // very efficient code to count characters in UTF string and ASCII string
    int32_t i = GetIthCharIndex(valueChars,length, ith);
    if (i < 0) return NULL;
    return &valueChars[i];
  }

  // Copy a value. If the value is inlined in a source tuple, then allocate
  // memory from the temp string pool and copy data there
  Value copyValue() const
  {
    Value copy = *this;
    if (m_sourceInlined) {
      // The Value storage is inlined (a pointer to the backing tuple storage) and needs
      // to be copied to a local storage
      copy.AllocateObjectFromInlinedValue(GetTempStringPool());
    }
    return copy;
  }

 private:
  /*
   * Private methods are private for a reason. Don't expose the raw
   * data so that it can be operated on directly.
   */

  // Function declarations for Value.cpp definitions.
  void CreateDecimalFromString(const std::string &txt);
  std::string CreateStringFromDecimal() const;

  // Helpers for InList.
  // These are purposely not inlines to avoid exposure of ValueList details.
  void DeserializeIntoANewValueList(SerializeInputBE &input, VarlenPool *dataPool);
  void AllocateANewValueList(size_t elementCount, ValueType elementType);

  // Promotion Rules. Initialized in Value.cpp
  static ValueType s_intPromotionTable[];
  static ValueType s_decimalPromotionTable[];
  static ValueType s_doublePromotionTable[];
  static TTInt s_maxDecimalValue;
  static TTInt s_minDecimalValue;
  // These initializers give the unique double values that are
  // closest but not equal to +/-1E26 within the accuracy of a double.
  static const double s_gtMaxDecimalAsDouble;
  static const double s_ltMinDecimalAsDouble;
  // These are the bound of converting decimal
  static TTInt s_maxInt64AsDecimal;
  static TTInt s_minInt64AsDecimal;

  /**
   * 16 bytes of storage for Value data.
   */
  char m_data[16];
  ValueType m_valueType;
  bool m_sourceInlined;
  bool m_cleanUp;

  /**
   * Private constructor that initializes storage and the specifies the type of value
   * that will be stored in this instance
   */
  Value(const ValueType type);

  /**
   * Set the type of the value that will be stored in this instance.
   * The last of the 16 bytes of storage allocated in an Value
   * is used to store the type
   */
  void SetValueType(ValueType type) {
    m_valueType = type;
  }

 public:

  /**
   * Get the type of the value. This information is private
   * to prevent code outside of Value from branching based on the type of a value.
   */
  ValueType GetValueType() const {
    return m_valueType;
  }

  void SetCleanUp(bool cleanup) {
    m_cleanUp = cleanup;
  }

 private:

  /**
   * Get the type of the value. This information is private
   * to prevent code outside of Value from branching based on the type of a value.
   */
  std::string GetValueTypeString() const {
    return ValueTypeToString(m_valueType);
  }

  void SetSourceInlined(bool sourceInlined)
  {
    m_sourceInlined = sourceInlined;
  }


  void tagAsNull() { m_data[13] = OBJECT_NULL_BIT; }

  /**
   * An Object is something Like a String that has a variable length
   * (thus it is length preceded) and can potentially have indirect
   * storage (will always be indirect when referenced via an Value).
   * Values cache a decoded version of the length preceding value
   * in their data area after the pointer to the object storage area.
   *
   * Leverage private access and enforce strict requirements on
   * Calling correctness.
   */
  int32_t GetObjectLengthWithoutNull() const {
    assert(IsNull() == false);
    assert(GetValueType() == VALUE_TYPE_VARCHAR || GetValueType() == VALUE_TYPE_VARBINARY);
    // now safe to read and return the length preceding value.
    return *reinterpret_cast<const int32_t *>(&m_data[8]);
  }


  int8_t SetObjectLength(int32_t length) {
    *reinterpret_cast<int32_t *>(&m_data[8]) = length;
    int8_t lengthLength = GetAppropriateObjectLengthLength(length);
    SetObjectLengthLength(lengthLength);
    return lengthLength;
  }

  /*
   * Retrieve the number of bytes used by the length preceding value
   * in the object's storage area. This value
   * is cached in the Value's 13th byte.
   */
  int8_t GetObjectLengthLength() const {
    return m_data[12];
  }

  /*
   * Set the objects length preceding values length to
   * the specified value
   */
  void SetObjectLengthLength(int8_t length) {
    m_data[12] = length;
  }

  /*
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

  /*
   * Set the length preceding value using the short or long representation depending
   * on what is necessary to represent the length.
   */
  static void SetObjectLengthToLocation(int32_t length, char *location) {
    int32_t beNumber = htonl(length);
    if (length < -1) {
      throw Exception("Object length cannot be < -1");
    } else if (length == -1) {
      location[0] = OBJECT_NULL_BIT;
    } if (length <= OBJECT_MAX_LENGTH_SHORT_LENGTH) {
      location[0] = reinterpret_cast<char*>(&beNumber)[3];
    } else {
      char *pointer = reinterpret_cast<char*>(&beNumber);
      location[0] = pointer[0];
      location[0] |= OBJECT_CONTINUATION_BIT;
      location[1] = pointer[1];
      location[2] = pointer[2];
      location[3] = pointer[3];
    }
  }

  /*
   * Not truly symmetrical with GetObjectValue which returns the actual object past
   * the length preceding value
   */
  void SetObjectValue(void* object) {
    *reinterpret_cast<void**>(m_data) = object;
  }

  void* GetObjectValueWithoutNull() const {
    void* value;
    if (m_sourceInlined) {
      value = *reinterpret_cast<char* const*>(m_data) + GetObjectLengthLength();
    }
    else {
      Varlen* sref = *reinterpret_cast<Varlen* const*>(m_data);
      value = sref->Get() + GetObjectLengthLength();
    }
    return value;
  }

  /**
   * Get a pointer to the value of an Object that lies beyond the storage of the length information
   */
  void* GetObjectValue() const {
    if (IsNull()) {
      return NULL;
    }
    return GetObjectValueWithoutNull();
  }

  // Getters
  const int8_t& GetTinyInt() const {
    assert(GetValueType() == VALUE_TYPE_TINYINT);
    return *reinterpret_cast<const int8_t*>(m_data);
  }

  int8_t& GetTinyInt() {
    assert(GetValueType() == VALUE_TYPE_TINYINT);
    return *reinterpret_cast<int8_t*>(m_data);
  }

  const int16_t& GetSmallInt() const {
    assert(GetValueType() == VALUE_TYPE_SMALLINT);
    return *reinterpret_cast<const int16_t*>(m_data);
  }

  int16_t& GetSmallInt() {
    assert(GetValueType() == VALUE_TYPE_SMALLINT);
    return *reinterpret_cast<int16_t*>(m_data);
  }

  const int32_t& GetInteger() const {
    assert(GetValueType() == VALUE_TYPE_INTEGER);
    return *reinterpret_cast<const int32_t*>(m_data);
  }

  int32_t& GetInteger() {
    assert(GetValueType() == VALUE_TYPE_INTEGER);
    return *reinterpret_cast<int32_t*>(m_data);
  }

  const int64_t& GetBigInt() const {
    assert((GetValueType() == VALUE_TYPE_BIGINT) ||
           (GetValueType() == VALUE_TYPE_TIMESTAMP) ||
           (GetValueType() == VALUE_TYPE_ADDRESS));
    return *reinterpret_cast<const int64_t*>(m_data);
  }

  int64_t& GetBigInt() {
    assert((GetValueType() == VALUE_TYPE_BIGINT) ||
           (GetValueType() == VALUE_TYPE_TIMESTAMP) ||
           (GetValueType() == VALUE_TYPE_ADDRESS));
    return *reinterpret_cast<int64_t*>(m_data);
  }

  const int64_t& GetTimestamp() const {
    assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
    return *reinterpret_cast<const int64_t*>(m_data);
  }

  int64_t& GetTimestamp() {
    assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
    return *reinterpret_cast<int64_t*>(m_data);
  }

  const double& GetDouble() const {
    assert(GetValueType() == VALUE_TYPE_DOUBLE);
    return *reinterpret_cast<const double*>(m_data);
  }

  double& GetDouble() {
    assert(GetValueType() == VALUE_TYPE_DOUBLE);
    return *reinterpret_cast<double*>(m_data);
  }

  const TTInt& GetDecimal() const {
    assert(GetValueType() == VALUE_TYPE_DECIMAL);
    const void* retval = reinterpret_cast<const void*>(m_data);
    return *reinterpret_cast<const TTInt*>(retval);
  }

  TTInt& GetDecimal() {
    assert(GetValueType() == VALUE_TYPE_DECIMAL);
    void* retval = reinterpret_cast<void*>(m_data);
    return *reinterpret_cast<TTInt*>(retval);
  }

  const bool& GetBoolean() const {
    assert(GetValueType() == VALUE_TYPE_BOOLEAN);
    return *reinterpret_cast<const bool*>(m_data);
  }

  bool& GetBoolean() {
    assert(GetValueType() == VALUE_TYPE_BOOLEAN);
    return *reinterpret_cast<bool*>(m_data);
  }

  bool IsBooleanNULL() const ;

  std::size_t GetAllocationSizeForObject() const;
  static std::size_t GetAllocationSizeForObject(int32_t length);

  static void ThrowCastSQLException(const ValueType origType,
                                    const ValueType newType)
  {
    char msg[1024];
    snprintf(msg, 1024, "Type %s can't be cast as %s",
             ValueTypeToString(origType).c_str(),
             ValueTypeToString(newType).c_str());
    throw TypeMismatchException(msg, origType, newType);
  }

  /** return the whole part of a TTInt*/
  static inline int64_t narrowDecimalToBigInt(TTInt &scaledValue) {
    if (scaledValue > Value::s_maxInt64AsDecimal || scaledValue < Value::s_minInt64AsDecimal) {
      ThrowCastSQLValueOutOfRangeException<TTInt>(scaledValue, VALUE_TYPE_DECIMAL, VALUE_TYPE_BIGINT);
    }
    TTInt whole(scaledValue);
    whole /= kMaxScaleFactor;
    return whole.ToInt();
  }

  /** return the fractional part of a TTInt*/
  static inline int64_t GetFractionalPart(TTInt& scaledValue) {
    TTInt fractional(scaledValue);
    fractional %= kMaxScaleFactor;
    return fractional.ToInt();
  }

  /**
   * Implicitly converting function to big integer type
   * DOUBLE, DECIMAL should not be handled here
   */
  int64_t CastAsBigIntAndGetValue() const {
    assert(IsNull() == false);

    const ValueType type = GetValueType();
    assert(type != VALUE_TYPE_NULL);
    switch (type) {
      case VALUE_TYPE_TINYINT:
        return static_cast<int64_t>(GetTinyInt());
      case VALUE_TYPE_SMALLINT:
        return static_cast<int64_t>(GetSmallInt());
      case VALUE_TYPE_INTEGER:
        return static_cast<int64_t>(GetInteger());
      case VALUE_TYPE_BIGINT:
        return GetBigInt();
      case VALUE_TYPE_TIMESTAMP:
        return GetTimestamp();
      default:
        ThrowCastSQLException(type, VALUE_TYPE_BIGINT);
        return 0; // NOT REACHED
    }
  }

  /**
   * Implicitly converting function to integer type
   * DOUBLE, DECIMAL should not be handled here
   */
  int32_t CastAsIntegerAndGetValue() const {
    assert(IsNull() == false);

    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_NULL:
        return INT32_NULL;
      case VALUE_TYPE_TINYINT:
        return static_cast<int32_t>(GetTinyInt());
      case VALUE_TYPE_SMALLINT:
        return static_cast<int32_t>(GetSmallInt());
      case VALUE_TYPE_INTEGER:
        return GetInteger();
      case VALUE_TYPE_BIGINT:
      {
        const int64_t value = GetBigInt();
        if (value > (int64_t)INT32_MAX || value < (int64_t)PELOTON_INT32_MIN) {
          ThrowCastSQLValueOutOfRangeException<int64_t>(value, VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
        }
        return static_cast<int32_t>(value);
      }
      default:
        ThrowCastSQLException(type, VALUE_TYPE_INTEGER);
        return 0; // NOT REACHED
    }
  }

  double CastAsDoubleAndGetValue() const {
    assert(IsNull() == false);

    const ValueType type = GetValueType();

    switch (type) {
      case VALUE_TYPE_NULL:
        return DOUBLE_MIN;
      case VALUE_TYPE_TINYINT:
        return static_cast<double>(GetTinyInt());
      case VALUE_TYPE_SMALLINT:
        return static_cast<double>(GetSmallInt());
      case VALUE_TYPE_INTEGER:
        return static_cast<double>(GetInteger());
      case VALUE_TYPE_ADDRESS:
        return static_cast<double>(GetBigInt());
      case VALUE_TYPE_BIGINT:
        return static_cast<double>(GetBigInt());
      case VALUE_TYPE_TIMESTAMP:
        return static_cast<double>(GetTimestamp());
      case VALUE_TYPE_DOUBLE:
        return GetDouble();
      case VALUE_TYPE_DECIMAL:
      {
        TTInt scaledValue = GetDecimal();
        // we only deal with the decimal number within int64_t range here
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        int64_t fractional = GetFractionalPart(scaledValue);
        double retval;
        retval = static_cast<double>(whole) +
            (static_cast<double>(fractional)/static_cast<double>(kMaxScaleFactor));
        return retval;
      }
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_DOUBLE);
        return 0; // NOT REACHED
    }
  }

  TTInt CastAsDecimalAndGetValue() const {
    assert(IsNull() == false);

    const ValueType type = GetValueType();

    switch (type) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      case VALUE_TYPE_TIMESTAMP: {
        int64_t value = CastAsBigIntAndGetValue();
        TTInt retval(value);
        retval *= kMaxScaleFactor;
        return retval;
      }
      case VALUE_TYPE_DECIMAL:
        return GetDecimal();
      case VALUE_TYPE_DOUBLE: {
        int64_t intValue = CastAsBigIntAndGetValue();
        TTInt retval(intValue);
        retval *= kMaxScaleFactor;

        double value = GetDouble();
        value -= static_cast<double>(intValue); // isolate decimal part
        value *= static_cast<double>(kMaxScaleFactor); // scale up to integer.
        TTInt fracval((int64_t)value);
        retval += fracval;
        return retval;
      }
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_DECIMAL);
        return 0; // NOT REACHED
    }
  }

  /**
   * This funciton does not check NULL value.
   */
  double GetNumberFromString() const
  {
    assert(IsNull() == false);

    const int32_t strLength = GetObjectLengthWithoutNull();
    // Guarantee termination at end of object -- or strtod might not stop there.
    char safeBuffer[strLength+1];
    memcpy(safeBuffer, GetObjectValueWithoutNull(), strLength);
    safeBuffer[strLength] = '\0';
    char * bufferEnd = safeBuffer;
    double result = strtod(safeBuffer, &bufferEnd);
    // Needs to have consumed SOMETHING.
    if (bufferEnd > safeBuffer) {
      // Unconsumed trailing chars are OK if they are whitespace.
      while (bufferEnd < safeBuffer+strLength && isspace(*bufferEnd)) {
        ++bufferEnd;
      }
      if (bufferEnd == safeBuffer+strLength) {
        return result;
      }
    }

    std::ostringstream oss;
    oss << "Could not convert to number: '" << safeBuffer << "' contains invalid character value.";
    throw Exception(oss.str());
  }

  Value CastAsBigInt() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_BIGINT);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetBigInt() = static_cast<int64_t>(GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        retval.GetBigInt() = static_cast<int64_t>(GetSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        retval.GetBigInt() = static_cast<int64_t>(GetInteger()); break;
      case VALUE_TYPE_ADDRESS:
        retval.GetBigInt() = GetBigInt(); break;
      case VALUE_TYPE_BIGINT:
        return *this;
      case VALUE_TYPE_TIMESTAMP:
        retval.GetBigInt() = GetTimestamp(); break;
      case VALUE_TYPE_DOUBLE:
        if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)PELOTON_INT64_MIN) {
          ThrowCastSQLValueOutOfRangeException<double>(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
        }
        retval.GetBigInt() = static_cast<int64_t>(GetDouble()); break;
      case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        retval.GetBigInt() = narrowDecimalToBigInt(scaledValue); break;
      }
      case VALUE_TYPE_VARCHAR:
        retval.GetBigInt() = static_cast<int64_t>(GetNumberFromString()); break;
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_BIGINT);
    }
    return retval;
  }

  Value CastAsTimestamp() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_TIMESTAMP);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetTimestamp() = static_cast<int64_t>(GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        retval.GetTimestamp() = static_cast<int64_t>(GetSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        retval.GetTimestamp() = static_cast<int64_t>(GetInteger()); break;
      case VALUE_TYPE_BIGINT:
        retval.GetTimestamp() = GetBigInt(); break;
      case VALUE_TYPE_TIMESTAMP:
        retval.GetTimestamp() = GetTimestamp(); break;
      case VALUE_TYPE_DOUBLE:
        // TODO: Consider just eliminating this switch case to throw a cast exception,
        // or explicitly throwing some other exception here.
        // Direct cast of double to timestamp (implemented via intermediate cast to integer, here)
        // is not a SQL standard requirement, may not even make it past the planner's type-checks,
        // or may just be too far a stretch.
        // OR it might be a convenience for some obscure system-generated edge case?

        if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)PELOTON_INT64_MIN) {
          ThrowCastSQLValueOutOfRangeException<double>(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
        }
        retval.GetTimestamp() = static_cast<int64_t>(GetDouble()); break;
      case VALUE_TYPE_DECIMAL: {
        // TODO: Consider just eliminating this switch case to throw a cast exception,
        // or explicitly throwing some other exception here.
        // Direct cast of decimal to timestamp (implemented via intermediate cast to integer, here)
        // is not a SQL standard requirement, may not even make it past the planner's type-checks,
        // or may just be too far a stretch.
        // OR it might be a convenience for some obscure system-generated edge case?

        TTInt scaledValue = GetDecimal();
        retval.GetTimestamp() = narrowDecimalToBigInt(scaledValue); break;
      }
      case VALUE_TYPE_VARCHAR: {
        const int32_t length = GetObjectLengthWithoutNull();
        const char* bytes = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
        const std::string value(bytes, length);
        retval.GetTimestamp() = parseTimestampString(value);
        break;
      }
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_TIMESTAMP);
    }
    return retval;
  }

  template <typename T>
  void narrowToInteger(const T value, ValueType sourceType)
  {
    if (value > (T)INT32_MAX || value < (T)PELOTON_INT32_MIN) {
      ThrowCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_INTEGER);
    }
    GetInteger() = static_cast<int32_t>(value);
  }

  Value CastAsInteger() const {
    Value retval(VALUE_TYPE_INTEGER);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetInteger() = static_cast<int32_t>(GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        retval.GetInteger() = static_cast<int32_t>(GetSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        return *this;
      case VALUE_TYPE_BIGINT:
        retval.narrowToInteger(GetBigInt(), type); break;
      case VALUE_TYPE_TIMESTAMP:
        retval.narrowToInteger(GetTimestamp(), type); break;
      case VALUE_TYPE_DOUBLE:
        retval.narrowToInteger(GetDouble(), type); break;
      case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        // Get the whole part of the decimal
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        // try to convert the whole part, which is a int64_t
        retval.narrowToInteger(whole, VALUE_TYPE_BIGINT); break;
      }
      case VALUE_TYPE_VARCHAR:
        retval.narrowToInteger(GetNumberFromString(), type); break;
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_INTEGER);
    }
    return retval;
  }

  template <typename T>
  void narrowToSmallInt(const T value, ValueType sourceType)
  {
    if (value > (T)INT16_MAX || value < (T)PELOTON_INT16_MIN) {
      ThrowCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_SMALLINT);
    }
    GetSmallInt() = static_cast<int16_t>(value);
  }

  Value CastAsSmallInt() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_SMALLINT);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetSmallInt() = static_cast<int16_t>(GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        retval.GetSmallInt() = GetSmallInt(); break;
      case VALUE_TYPE_INTEGER:
        retval.narrowToSmallInt(GetInteger(), type); break;
      case VALUE_TYPE_BIGINT:
        retval.narrowToSmallInt(GetBigInt(), type); break;
      case VALUE_TYPE_TIMESTAMP:
        retval.narrowToSmallInt(GetTimestamp(), type); break;
      case VALUE_TYPE_DOUBLE:
        retval.narrowToSmallInt(GetDouble(), type); break;
      case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        retval.narrowToSmallInt(whole, VALUE_TYPE_BIGINT); break;
      }
      case VALUE_TYPE_VARCHAR:
        retval.narrowToSmallInt(GetNumberFromString(), type); break;
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_SMALLINT);
    }
    return retval;
  }

  template <typename T>
  void narrowToTinyInt(const T value, ValueType sourceType)
  {
    if (value > (T)INT8_MAX || value < (T)PELOTON_INT8_MIN) {
      ThrowCastSQLValueOutOfRangeException(value, sourceType, VALUE_TYPE_TINYINT);
    }
    GetTinyInt() = static_cast<int8_t>(value);
  }

  Value CastAsTinyInt() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_TINYINT);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetTinyInt() = GetTinyInt(); break;
      case VALUE_TYPE_SMALLINT:
        retval.narrowToTinyInt(GetSmallInt(), type); break;
      case VALUE_TYPE_INTEGER:
        retval.narrowToTinyInt(GetInteger(), type); break;
      case VALUE_TYPE_BIGINT:
        retval.narrowToTinyInt(GetBigInt(), type); break;
      case VALUE_TYPE_TIMESTAMP:
        retval.narrowToTinyInt(GetTimestamp(), type); break;
      case VALUE_TYPE_DOUBLE:
        retval.narrowToTinyInt(GetDouble(), type); break;
      case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        retval.narrowToTinyInt(whole, type); break;
      }
      case VALUE_TYPE_VARCHAR:
        retval.narrowToTinyInt(GetNumberFromString(), type); break;
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_TINYINT);
    }
    return retval;
  }

  Value CastAsDouble() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_DOUBLE);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT:
        retval.GetDouble() = static_cast<double>(GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        retval.GetDouble() = static_cast<double>(GetSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        retval.GetDouble() = static_cast<double>(GetInteger()); break;
      case VALUE_TYPE_BIGINT:
        retval.GetDouble() = static_cast<double>(GetBigInt()); break;
      case VALUE_TYPE_TIMESTAMP:
        retval.GetDouble() = static_cast<double>(GetTimestamp()); break;
      case VALUE_TYPE_DOUBLE:
        retval.GetDouble() = GetDouble(); break;
      case VALUE_TYPE_DECIMAL:
        retval.GetDouble() = CastAsDoubleAndGetValue(); break;
      case VALUE_TYPE_VARCHAR:
        retval.GetDouble() = GetNumberFromString(); break;
      case VALUE_TYPE_VARBINARY:
      default:
        ThrowCastSQLException(type, VALUE_TYPE_DOUBLE);
    }
    return retval;
  }

  void streamTimestamp(std::stringstream& value) const;

  Value CastAsString() const {
    assert(IsNull() == false);

    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_TINYINT: {
        std::stringstream value;
        // This cast keeps the tiny int from being confused for a char.
        value << static_cast<int>(GetTinyInt());
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      case VALUE_TYPE_SMALLINT: {
        std::stringstream value;
        value << GetSmallInt();
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      case VALUE_TYPE_INTEGER: {
        std::stringstream value;
        value << GetInteger();
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      case VALUE_TYPE_BIGINT: {
        std::stringstream value;
        value << GetBigInt();
        return GetTempStringValue(value.str().c_str(), value.str().length());
        //case VALUE_TYPE_TIMESTAMP:
        //TODO: The SQL standard wants an actual date literal rather than a numeric value, here. See ENG-4284.
        //value << static_cast<double>(GetTimestamp()); break;
      }
      case VALUE_TYPE_DOUBLE: {
        std::stringstream value;
        // Use the specific standard SQL formatting for float values,
        // which the C/C++ format options don't quite support.
        StreamSQLFloatFormat(value, GetDouble());
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      case VALUE_TYPE_DECIMAL: {
        std::stringstream value;
        value << CreateStringFromDecimal();
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_VARBINARY: {
        // note: we allow binary conversion to strings to support
        // byte[] as string parameters...
        // In the future, it would be nice to check this is a decent string here...
        //Peloton Changes:
        //Value retval(VALUE_TYPE_VARCHAR);
        //memcpy(retval.m_data, m_data, sizeof(m_data));
        return *this;
      }
      case VALUE_TYPE_TIMESTAMP: {
        std::stringstream value;
        streamTimestamp(value);
        return GetTempStringValue(value.str().c_str(), value.str().length());
      }
      default:
        ThrowCastSQLException(type, VALUE_TYPE_VARCHAR);
    }
    return *this;
  }

  Value CastAsBinary() const {
    assert(IsNull() == false);

    Value retval(VALUE_TYPE_VARBINARY);
    const ValueType type = GetValueType();
    switch (type) {
      case VALUE_TYPE_VARBINARY:
        memcpy(retval.m_data, m_data, sizeof(m_data));
        break;
      default:
        ThrowCastSQLException(type, VALUE_TYPE_VARBINARY);
    }
    return retval;
  }

  void CreateDecimalFromInt(int64_t rhsint)
  {
    TTInt scaled(rhsint);
    scaled *= kMaxScaleFactor;
    GetDecimal() = scaled;
  }

  Value CastAsDecimal() const {
    assert(IsNull() == false);
    Value retval(VALUE_TYPE_DECIMAL);
    const ValueType type = GetValueType();
    if (IsNull()) {
      retval.SetNull();
      return retval;
    }
    switch (type) {
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_INTEGER:
      case VALUE_TYPE_BIGINT:
      {
        int64_t rhsint = CastAsBigIntAndGetValue();
        retval.CreateDecimalFromInt(rhsint);
        break;
      }
      case VALUE_TYPE_DECIMAL:
        ::memcpy(retval.m_data, m_data, sizeof(TTInt));
        break;
      case VALUE_TYPE_DOUBLE:
      {
        const double& value = GetDouble();
        if (value >= s_gtMaxDecimalAsDouble || value <= s_ltMinDecimalAsDouble) {
          char message[4096];
          snprintf(message, 4096, "Attempted to cast value %f causing overflow/underflow", value);
          throw Exception(message);
        }
        // Resort to string as the intermediary since even int64_t does not cover the full range.
        char decimalAsString[41]; // Large enough to account for digits, sign, decimal, and terminating null.
        snprintf(decimalAsString, sizeof(decimalAsString), "%.12f", value);
        // Shift the entire integer part 1 digit to the right, overwriting the decimal point.
        // This effectively Creates a potentially very large integer value
        //  equal to the original double scaled up by 10^12.
        for (char* intDigit = strchr(decimalAsString, '.'); intDigit > decimalAsString; --intDigit) {
          *intDigit = *(intDigit-1);
        }
        TTInt result(decimalAsString+1);
        retval.GetDecimal() = result;
        break;
      }
      case VALUE_TYPE_VARCHAR:
      {
        const int32_t length = GetObjectLengthWithoutNull();
        const char* bytes = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
        const std::string value(bytes, length);
        retval.CreateDecimalFromString(value);
        break;
      }
      default:
        ThrowCastSQLException(type, VALUE_TYPE_DECIMAL);
    }
    return retval;
  }

  /**
   * Copy the arbitrary size object that this value points to as an
   * inline object in the provided storage area
   */
  void InlineCopyyObject(void *storage, int32_t maxLength, bool isInBytes) const {
    if (IsNull()) {
      // Always reSet all the bits regardless of the actual length of the value
      // 1 additional byte for the length prefix
      ::memset(storage, 0, maxLength + 1);

      /*
       * The 7th bit of the length preceding value
       * is used to indicate that the object is null.
       */
      *reinterpret_cast<char*>(storage) = OBJECT_NULL_BIT;
    }
    else {
      const int32_t objLength = GetObjectLengthWithoutNull();
      const char* ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
      checkTooNarrowVarcharAndVarbinary(m_valueType, ptr, objLength, maxLength, isInBytes);

      // Always reSet all the bits regardless of the actual length of the value
      // 1 additional byte for the length prefix
      ::memset(storage, 0, maxLength + 1);

      if (m_sourceInlined)
      {
        ::memcpy( storage, *reinterpret_cast<char *const *>(m_data), GetObjectLengthLength() + objLength);
      }
      else
      {
        const Varlen* sref =
            *reinterpret_cast<Varlen* const*>(m_data);
        ::memcpy(storage, sref->Get(),
                 GetObjectLengthLength() + objLength);
      }
    }

  }

  static inline bool validVarcharSize(const char *valueChars, const size_t length, const int32_t maxLength) {
    int32_t min_continuation_bytes = static_cast<int32_t>(length - maxLength);
    if (min_continuation_bytes <= 0) {
      return true;
    }
    size_t i = length;
    while (i--) {
      if ((valueChars[i] & 0xc0) == 0x80) {
        if (--min_continuation_bytes == 0) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Assuming non-null Value, validate the size of the varchar or varbinary
   */
  static inline void checkTooNarrowVarcharAndVarbinary(ValueType type, const char* ptr,
                                                       int32_t objLength, int32_t maxLength, bool isInBytes) {
    if (maxLength == 0) {
      throw ObjectSizeException("Zero maxLength for object type" + ValueTypeToString(type));
    }

    if (type == VALUE_TYPE_VARBINARY) {
      if (objLength > maxLength) {
        char msg[1024];
        snprintf(msg, 1024,
                 "The size %d of the value exceeds the size of the VARBINARY(%d) column.",
                 objLength, maxLength);
        throw ObjectSizeException(msg);
      }
    } else if (type == VALUE_TYPE_VARCHAR) {
      if (isInBytes) {
        if (objLength > maxLength) {
          std::string inputValue;
          if (objLength > FULL_STRING_IN_MESSAGE_THRESHOLD) {
            inputValue = std::string(ptr, FULL_STRING_IN_MESSAGE_THRESHOLD) + std::string("...");
          } else {
            inputValue = std::string(ptr, objLength);
          }
          char msg[1024];
          snprintf(msg, 1024,
                   "The size %d of the value '%s' exceeds the size of the VARCHAR(%d BYTES) column.",
                   objLength, inputValue.c_str(), maxLength);
          throw ObjectSizeException(msg);
        }
      } else if (!validVarcharSize(ptr, objLength, maxLength)) {
        const int32_t charLength = GetCharLength(ptr, objLength);
        char msg[1024];
        std::string inputValue;
        if (charLength > FULL_STRING_IN_MESSAGE_THRESHOLD) {
          const char * end = GetIthCharPosition(ptr, objLength, FULL_STRING_IN_MESSAGE_THRESHOLD+1);
          int32_t numBytes = (int32_t)(end - ptr);
          inputValue = std::string(ptr, numBytes) + std::string("...");
        } else {
          inputValue = std::string(ptr, objLength);
        }
        snprintf(msg, 1024,
                 "The size %d of the value '%s' exceeds the size of the VARCHAR(%d) column.",
                 charLength, inputValue.c_str(), maxLength);

        throw ObjectSizeException(msg);
      }
    } else {
      throw IncompatibleTypeException(type, "Invalid object type for this function");
    }
  }

  template<typename T>
  int CompareValue (const T lhsValue, const T rhsValue) const {
    if (lhsValue == rhsValue) {
      return VALUE_COMPARE_EQUAL;
    } else if (lhsValue > rhsValue) {
      return VALUE_COMPARE_GREATERTHAN;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  }

  int CompareDoubleValue (const double lhsValue, const double rhsValue) const {
    // Treat NaN values as equals and also make them smaller than neagtive infinity.
    // This breaks IEEE754 for expressions slightly.
    if (std::isnan(lhsValue)) {
      return std::isnan(rhsValue) ? VALUE_COMPARE_EQUAL : VALUE_COMPARE_LESSTHAN;
    }
    else if (std::isnan(rhsValue)) {
      return VALUE_COMPARE_GREATERTHAN;
    }
    else if (lhsValue > rhsValue) {
      return VALUE_COMPARE_GREATERTHAN;
    }
    else if (lhsValue < rhsValue) {
      return VALUE_COMPARE_LESSTHAN;
    }
    else {
      return VALUE_COMPARE_EQUAL;
    }
  }

  int CompareTinyInt (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_TINYINT);

    // Get the right hand side as a bigint
    if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
      return CompareDoubleValue(static_cast<double>(GetTinyInt()), rhs.GetDouble());
    } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
      const TTInt rhsValue = rhs.GetDecimal();
      TTInt lhsValue(static_cast<int64_t>(GetTinyInt()));
      lhsValue *= kMaxScaleFactor;
      return CompareValue<TTInt>(lhsValue, rhsValue);
    } else {
      int64_t lhsValue, rhsValue;
      lhsValue = static_cast<int64_t>(GetTinyInt());
      rhsValue = rhs.CastAsBigIntAndGetValue();
      return CompareValue<int64_t>(lhsValue, rhsValue);
    }
  }

  int CompareSmallInt (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_SMALLINT);

    // Get the right hand side as a bigint
    if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
      return CompareDoubleValue(static_cast<double>(GetSmallInt()), rhs.GetDouble());
    } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
      const TTInt rhsValue = rhs.GetDecimal();
      TTInt lhsValue(static_cast<int64_t>(GetSmallInt()));
      lhsValue *= kMaxScaleFactor;
      return CompareValue<TTInt>(lhsValue, rhsValue);
    } else {
      int64_t lhsValue, rhsValue;
      lhsValue = static_cast<int64_t>(GetSmallInt());
      rhsValue = rhs.CastAsBigIntAndGetValue();
      return CompareValue<int64_t>(lhsValue, rhsValue);
    }
  }

  int CompareInteger (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_INTEGER);

    // Get the right hand side as a bigint
    if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
      return CompareDoubleValue(static_cast<double>(GetInteger()), rhs.GetDouble());
    } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
      const TTInt rhsValue = rhs.GetDecimal();
      TTInt lhsValue(static_cast<int64_t>(GetInteger()));
      lhsValue *= kMaxScaleFactor;
      return CompareValue<TTInt>(lhsValue, rhsValue);
    } else {
      int64_t lhsValue, rhsValue;
      lhsValue = static_cast<int64_t>(GetInteger());
      rhsValue = rhs.CastAsBigIntAndGetValue();
      return CompareValue<int64_t>(lhsValue, rhsValue);
    }
  }

  int CompareBigInt (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_BIGINT);

    // Get the right hand side as a bigint
    if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
      return CompareDoubleValue(static_cast<double>(GetBigInt()), rhs.GetDouble());
    } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
      const TTInt rhsValue = rhs.GetDecimal();
      TTInt lhsValue(GetBigInt());
      lhsValue *= kMaxScaleFactor;
      return CompareValue<TTInt>(lhsValue, rhsValue);
    } else {
      int64_t lhsValue, rhsValue;
      lhsValue = GetBigInt();
      rhsValue = rhs.CastAsBigIntAndGetValue();
      return CompareValue<int64_t>(lhsValue, rhsValue);
    }
  }


  int CompareTimestamp (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_TIMESTAMP);

    // Get the right hand side as a bigint
    if (rhs.GetValueType() == VALUE_TYPE_DOUBLE) {
      return CompareDoubleValue(static_cast<double>(GetTimestamp()), rhs.GetDouble());
    } else if (rhs.GetValueType() == VALUE_TYPE_DECIMAL) {
      const TTInt rhsValue = rhs.GetDecimal();
      TTInt lhsValue(GetTimestamp());
      lhsValue *= kMaxScaleFactor;
      return CompareValue<TTInt>(lhsValue, rhsValue);
    } else {
      int64_t lhsValue, rhsValue;
      lhsValue = GetTimestamp();
      rhsValue = rhs.CastAsBigIntAndGetValue();
      return CompareValue<int64_t>(lhsValue, rhsValue);
    }
  }

  int CompareDoubleValue (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_DOUBLE);

    const double lhsValue = GetDouble();
    double rhsValue;

    switch (rhs.GetValueType()) {
      case VALUE_TYPE_DOUBLE:
        rhsValue = rhs.GetDouble(); break;
      case VALUE_TYPE_TINYINT:
        rhsValue = static_cast<double>(rhs.GetTinyInt()); break;
      case VALUE_TYPE_SMALLINT:
        rhsValue = static_cast<double>(rhs.GetSmallInt()); break;
      case VALUE_TYPE_INTEGER:
        rhsValue = static_cast<double>(rhs.GetInteger()); break;
      case VALUE_TYPE_BIGINT:
        rhsValue = static_cast<double>(rhs.GetBigInt()); break;
      case VALUE_TYPE_TIMESTAMP:
        rhsValue = static_cast<double>(rhs.GetTimestamp()); break;
      case VALUE_TYPE_DECIMAL:
      {
        TTInt scaledValue = rhs.GetDecimal();
        TTInt whole(scaledValue);
        TTInt fractional(scaledValue);
        whole /= kMaxScaleFactor;
        fractional %= kMaxScaleFactor;
        rhsValue = static_cast<double>(whole.ToInt()) +
            (static_cast<double>(fractional.ToInt())/static_cast<double>(kMaxScaleFactor));
        break;
      }
      default:
        char message[128];
        snprintf(message, 128,
                 "Type %s cannot be cast for comparison to type %s",
                 ValueTypeToString(rhs.GetValueType()).c_str(),
                 ValueTypeToString(GetValueType()).c_str());
        throw TypeMismatchException(message, rhs.GetValueType(), GetValueType());
        // Not reached
    }

    return CompareDoubleValue(lhsValue, rhsValue);
  }

  int CompareStringValue (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_VARCHAR);

    ValueType rhsType = rhs.GetValueType();
    if ((rhsType != VALUE_TYPE_VARCHAR) && (rhsType != VALUE_TYPE_VARBINARY)) {
      char message[128];
      snprintf(message, 128,
               "Type %s cannot be cast for comparison to type %s",
               ValueTypeToString(rhsType).c_str(),
               ValueTypeToString(m_valueType).c_str());
      throw TypeMismatchException(message, rhs.GetValueType(), GetValueType());
    }

    assert(m_valueType == VALUE_TYPE_VARCHAR);

    const int32_t leftLength = GetObjectLengthWithoutNull();
    const int32_t rightLength = rhs.GetObjectLengthWithoutNull();
    const char* left = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
    const char* right = reinterpret_cast<const char*>(rhs.GetObjectValueWithoutNull());

    const int result = ::strncmp(left, right, std::min(leftLength, rightLength));
    if (result == 0 && leftLength != rightLength) {
      if (leftLength > rightLength) {
        return  VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    else if (result > 0) {
      return VALUE_COMPARE_GREATERTHAN;
    }
    else if (result < 0) {
      return VALUE_COMPARE_LESSTHAN;
    }

    return VALUE_COMPARE_EQUAL;
  }

  int CompareBinaryValue (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_VARBINARY);

    if (rhs.GetValueType() != VALUE_TYPE_VARBINARY) {
      char message[128];
      snprintf(message, 128,
               "Type %s cannot be cast for comparison to type %s",
               ValueTypeToString(rhs.GetValueType()).c_str(),
               ValueTypeToString(m_valueType).c_str());
      throw TypeMismatchException(message, rhs.GetValueType(), m_valueType);
    }
    const int32_t leftLength = GetObjectLengthWithoutNull();
    const int32_t rightLength = rhs.GetObjectLengthWithoutNull();

    const char* left = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
    const char* right = reinterpret_cast<const char*>(rhs.GetObjectValueWithoutNull());

    const int result = ::memcmp(left, right, std::min(leftLength, rightLength));
    if (result == 0 && leftLength != rightLength) {
      if (leftLength > rightLength) {
        return  VALUE_COMPARE_GREATERTHAN;
      } else {
        return VALUE_COMPARE_LESSTHAN;
      }
    }
    else if (result > 0) {
      return VALUE_COMPARE_GREATERTHAN;
    }
    else if (result < 0) {
      return VALUE_COMPARE_LESSTHAN;
    }

    return VALUE_COMPARE_EQUAL;
  }

  int CompareDecimalValue (const Value rhs) const {
    assert(m_valueType == VALUE_TYPE_DECIMAL);
    switch (rhs.GetValueType()) {
      case VALUE_TYPE_DECIMAL:
      {
        return CompareValue<TTInt>(GetDecimal(), rhs.GetDecimal());
      }
      case VALUE_TYPE_DOUBLE:
      {
        const double rhsValue = rhs.GetDouble();
        TTInt scaledValue = GetDecimal();
        TTInt whole(scaledValue);
        TTInt fractional(scaledValue);
        whole /= kMaxScaleFactor;
        fractional %= kMaxScaleFactor;
        const double lhsValue = static_cast<double>(whole.ToInt()) +
            (static_cast<double>(fractional.ToInt())/static_cast<double>(kMaxScaleFactor));

        return CompareValue<double>(lhsValue, rhsValue);
      }
      // Create the equivalent decimal value
      case VALUE_TYPE_TINYINT:
      {
        TTInt rhsValue(static_cast<int64_t>(rhs.GetTinyInt()));
        rhsValue *= kMaxScaleFactor;
        return CompareValue<TTInt>(GetDecimal(), rhsValue);
      }
      case VALUE_TYPE_SMALLINT:
      {
        TTInt rhsValue(static_cast<int64_t>(rhs.GetSmallInt()));
        rhsValue *= kMaxScaleFactor;
        return CompareValue<TTInt>(GetDecimal(), rhsValue);
      }
      case VALUE_TYPE_INTEGER:
      {
        TTInt rhsValue(static_cast<int64_t>(rhs.GetInteger()));
        rhsValue *= kMaxScaleFactor;
        return CompareValue<TTInt>(GetDecimal(), rhsValue);
      }
      case VALUE_TYPE_BIGINT:
      {
        TTInt rhsValue(rhs.GetBigInt());
        rhsValue *= kMaxScaleFactor;
        return CompareValue<TTInt>(GetDecimal(), rhsValue);
      }
      case VALUE_TYPE_TIMESTAMP:
      {
        TTInt rhsValue(rhs.GetTimestamp());
        rhsValue *= kMaxScaleFactor;
        return CompareValue<TTInt>(GetDecimal(), rhsValue);
      }
      default:
      {
        char message[128];
        snprintf(message, 128,
                 "Type %s cannot be cast for comparison to type %s",
                 ValueTypeToString(rhs.GetValueType()).c_str(),
                 ValueTypeToString(GetValueType()).c_str());
        throw TypeMismatchException(message, rhs.GetValueType(), GetValueType());
        // Not reached
        return 0;
      }
    }
  }

  Value OpAddBigInts(const int64_t lhs, const int64_t rhs) const {
    //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
    if ( ((lhs^rhs)
        | (((lhs^(~(lhs^rhs)
            & (1L << (sizeof(int64_t)*CHAR_BIT-1))))+rhs)^rhs)) >= 0) {
      char message[4096];
      snprintf(message, 4096, "Adding %jd and %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
      throw Exception(message);
    }
    return GetBigIntValue(lhs + rhs);
  }

  Value OpSubtractBigInts(const int64_t lhs, const int64_t rhs) const {
    //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
    if ( ((lhs^rhs)
        & (((lhs ^ ((lhs^rhs)
            & (1L << (sizeof(int64_t)*CHAR_BIT-1))))-rhs)^rhs)) < 0) {
      char message[4096];
      snprintf(message, 4096, "Subtracting %jd from %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
      throw Exception(message);
    }
    return GetBigIntValue(lhs - rhs);
  }

  Value OpMultiplyBigInts(const int64_t lhs, const int64_t rhs) const {
    bool overflow = false;
    //Scary overflow check from https://www.securecoding.cert.org/confluence/display/cplusplus/INT32-CPP.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow
    if (lhs > 0){  /* lhs is positive */
      if (rhs > 0) {  /* lhs and rhs are positive */
        if (lhs > (INT64_MAX / rhs)) {
          overflow= true;
        }
      } /* end if lhs and rhs are positive */
      else { /* lhs positive, rhs non-positive */
        if (rhs < (INT64_MIN / lhs)) {
          overflow = true;
        }
      } /* lhs positive, rhs non-positive */
    } /* end if lhs is positive */
    else { /* lhs is non-positive */
      if (rhs > 0) { /* lhs is non-positive, rhs is positive */
        if (lhs < (INT64_MIN / rhs)) {
          overflow = true;
        }
      } /* end if lhs is non-positive, rhs is positive */
      else { /* lhs and rhs are non-positive */
        if ( (lhs != 0) && (rhs < (INT64_MAX / lhs))) {
          overflow = true;
        }
      } /* end if lhs and rhs non-positive */
    } /* end if lhs is non-positive */

    const int64_t result = lhs * rhs;

    if (result == INT64_NULL) {
      overflow = true;
    }

    if (overflow) {
      char message[4096];
      snprintf(message, 4096, "Multiplying %jd with %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
      throw Exception(message);
    }

    return GetBigIntValue(result);
  }

  Value OpDivideBigInts(const int64_t lhs, const int64_t rhs) const {
    if (rhs == 0) {
      char message[4096];
      snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
      throw Exception(message);
    }

    /**
     * Because the smallest int64 value is used to represent null (and this is checked for an handled above)
     * it isn't necessary to check for any kind of overflow since none is possible.
     */
    return GetBigIntValue(int64_t(lhs / rhs));
  }

  Value OpAddDoubles(const double lhs, const double rhs) const {
    const double result = lhs + rhs;
    ThrowDataExceptionIfInfiniteOrNaN(result, "'+' operator");
    return GetDoubleValue(result);
  }

  Value OpSubtractDoubles(const double lhs, const double rhs) const {
    const double result = lhs - rhs;
    ThrowDataExceptionIfInfiniteOrNaN(result, "'-' operator");
    return GetDoubleValue(result);
  }

  Value OpMultiplyDoubles(const double lhs, const double rhs) const {
    const double result = lhs * rhs;
    ThrowDataExceptionIfInfiniteOrNaN(result, "'*' operator");
    return GetDoubleValue(result);
  }

  Value OpDivideDoubles(const double lhs, const double rhs) const {
    const double result = lhs / rhs;
    ThrowDataExceptionIfInfiniteOrNaN(result, "'/' operator");
    return GetDoubleValue(result);
  }

  Value OpAddDecimals(const Value &lhs, const Value &rhs) const {
    assert(lhs.IsNull() == false);
    assert(rhs.IsNull() == false);
    assert(lhs.GetValueType() == VALUE_TYPE_DECIMAL);
    assert(rhs.GetValueType() == VALUE_TYPE_DECIMAL);

    TTInt retval(lhs.GetDecimal());
    if (retval.Add(rhs.GetDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
      char message[4096];
      snprintf(message, 4096, "Attempted to add %s with %s causing overflow/underflow",
               lhs.CreateStringFromDecimal().c_str(), rhs.CreateStringFromDecimal().c_str());
      throw Exception(message);
    }

    return GetDecimalValue(retval);
  }

  Value OpSubtractDecimals(const Value &lhs, const Value &rhs) const {
    assert(lhs.IsNull() == false);
    assert(rhs.IsNull() == false);
    assert(lhs.GetValueType() == VALUE_TYPE_DECIMAL);
    assert(rhs.GetValueType() == VALUE_TYPE_DECIMAL);

    TTInt retval(lhs.GetDecimal());
    if (retval.Sub(rhs.GetDecimal()) || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
      char message[4096];
      snprintf(message, 4096, "Attempted to subtract %s from %s causing overflow/underflow",
               rhs.CreateStringFromDecimal().c_str(), lhs.CreateStringFromDecimal().c_str());
      throw Exception(message);
    }

    return GetDecimalValue(retval);
  }

  /*
   * Avoid scaling both sides if possible. E.g, don't turn dec * 2 into
   * (dec * 2*kMaxScale*E-12). Then the result of simple multiplication
   * is a*b*E-24 and have to further multiply to Get back to the assumed
   * E-12, which can overflow unnecessarily at the middle step.
   */
  Value OpMultiplyDecimals(const Value &lhs, const Value &rhs) const {
    assert(lhs.IsNull() == false);
    assert(rhs.IsNull() == false);
    assert(lhs.GetValueType() == VALUE_TYPE_DECIMAL);
    assert(rhs.GetValueType() == VALUE_TYPE_DECIMAL);

    TTLInt calc;
    calc.FromInt(lhs.GetDecimal());
    calc *= rhs.GetDecimal();
    calc /= kMaxScaleFactor;
    TTInt retval;
    if (retval.FromInt(calc)  || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
      char message[4096];
      snprintf(message, 4096, "Attempted to multiply %s by %s causing overflow/underflow. Unscaled result was %s",
               lhs.CreateStringFromDecimal().c_str(), rhs.CreateStringFromDecimal().c_str(),
               calc.ToString(10).c_str());
      throw Exception(message);
    }
    return GetDecimalValue(retval);
  }


  /*
   * Divide two decimals and return a correctly scaled decimal.
   * A little cumbersome. Better algorithms welcome.
   *   (1) calculate the quotient and the remainder.
   *   (2) temporarily scale the remainder to 19 digits
   *   (3) divide out remainder to calculate digits after the radix point.
   *   (4) scale remainder to 12 digits (that's the default scale)
   *   (5) scale the quotient back to 19,12.
   *   (6) sum the scaled quotient and remainder.
   *   (7) construct the final decimal.
   */

  Value OpDivideDecimals(const Value &lhs, const Value &rhs) const {
    assert(lhs.IsNull() == false);
    assert(rhs.IsNull() == false);
    assert(lhs.GetValueType() == VALUE_TYPE_DECIMAL);
    assert(rhs.GetValueType() == VALUE_TYPE_DECIMAL);

    TTLInt calc;
    calc.FromInt(lhs.GetDecimal());
    calc *= kMaxScaleFactor;
    if (calc.Div(rhs.GetDecimal())) {
      char message[4096];
      snprintf( message, 4096, "Attempted to divide %s by %s causing overflow/underflow (or divide by zero)",
                lhs.CreateStringFromDecimal().c_str(), rhs.CreateStringFromDecimal().c_str());
      throw Exception(message);
    }
    TTInt retval;
    if (retval.FromInt(calc)  || retval > s_maxDecimalValue || retval < s_minDecimalValue) {
      char message[4096];
      snprintf( message, 4096, "Attempted to divide %s by %s causing overflow. Unscaled result was %s",
                lhs.CreateStringFromDecimal().c_str(), rhs.CreateStringFromDecimal().c_str(),
                calc.ToString(10).c_str());
      throw Exception(message);
    }
    return GetDecimalValue(retval);
  }

  static Value GetTinyIntValue(int8_t value) {
    Value retval(VALUE_TYPE_TINYINT);
    retval.GetTinyInt() = value;
    if (value == INT8_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetSmallIntValue(int16_t value) {
    Value retval(VALUE_TYPE_SMALLINT);
    retval.GetSmallInt() = value;
    if (value == INT16_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetIntegerValue(int32_t value) {
    Value retval(VALUE_TYPE_INTEGER);
    retval.GetInteger() = value;
    if (value == INT32_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetBigIntValue(int64_t value) {
    Value retval(VALUE_TYPE_BIGINT);
    retval.GetBigInt() = value;
    if (value == INT64_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetTimestampValue(int64_t value) {
    Value retval(VALUE_TYPE_TIMESTAMP);
    retval.GetTimestamp() = value;
    if (value == INT64_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetDoubleValue(double value) {
    Value retval(VALUE_TYPE_DOUBLE);
    retval.GetDouble() = value;
    if (value <= DOUBLE_NULL) {
      retval.tagAsNull();
    }
    return retval;
  }

  static Value GetBooleanValue(bool value) {
    Value retval(VALUE_TYPE_BOOLEAN);
    retval.GetBoolean() = value;
    return retval;
  }

  static Value GetDecimalValueFromString(const std::string &value) {
    Value retval(VALUE_TYPE_DECIMAL);
    retval.CreateDecimalFromString(value);
    return retval;
  }

  static Value GetAllocatedArrayValueFromSizeAndType(size_t elementCount, ValueType elementType)
  {
    Value retval(VALUE_TYPE_ARRAY);
    retval.AllocateANewValueList(elementCount, elementType);
    return retval;
  }

  static VarlenPool* GetTempStringPool() {
    // TODO: For now, allocate out of heap !
    return nullptr;
  }

  static Value GetTempStringValue(const char* value, size_t size) {
    return GetAllocatedValue(VALUE_TYPE_VARCHAR, value, size, GetTempStringPool());
  }

  static Value GetTempBinaryValue(const unsigned char* value, size_t size) {
    return GetAllocatedValue(VALUE_TYPE_VARBINARY, reinterpret_cast<const char*>(value), size, GetTempStringPool());
  }

  /// Assumes hex-encoded input
  static inline Value GetTempBinaryValueFromHex(const std::string& value) {
    size_t rawLength = value.length() / 2;
    unsigned char rawBuf[rawLength];
    HexDecodeToBinary(rawBuf, value.c_str());
    return GetTempBinaryValue(rawBuf, rawLength);
  }

  static Value GetAllocatedValue(ValueType type, const char* value, size_t size, VarlenPool* stringPool) {
    Value retval(type);
    char* storage = retval.AllocateValueStorage((int32_t)size, stringPool);
    ::memcpy(storage, value, (int32_t)size);
    retval.SetSourceInlined(false);
    retval.SetCleanUp(stringPool == nullptr);
    return retval;
  }

  char* AllocateValueStorage(int32_t length, VarlenPool* stringPool)
  {
    // This unSets the Value's null tag and returns the length of the length.
    const int8_t lengthLength = SetObjectLength(length);
    const int32_t minLength = length + lengthLength;
    Varlen* sref = Varlen::Create(minLength, stringPool);
    char* storage = sref->Get();
    SetObjectLengthToLocation(length, storage);
    storage += lengthLength;
    SetObjectValue(sref);
    return storage;
  }

  static Value GetNullStringValue() {
    Value retval(VALUE_TYPE_VARCHAR);
    retval.tagAsNull();
    *reinterpret_cast<char**>(retval.m_data) = NULL;
    return retval;
  }

  static Value GetNullBinaryValue() {
    Value retval(VALUE_TYPE_VARBINARY);
    retval.tagAsNull();
    *reinterpret_cast<char**>(retval.m_data) = NULL;
    return retval;
  }

  static Value GetNullValue() {
    Value retval(VALUE_TYPE_NULL);
    retval.tagAsNull();
    return retval;
  }

  static Value GetDecimalValue(TTInt value) {
    Value retval(VALUE_TYPE_DECIMAL);
    retval.GetDecimal() = value;
    return retval;
  }

  static Value GetAddressValue(void *address) {
    Value retval(VALUE_TYPE_ADDRESS);
    *reinterpret_cast<void**>(retval.m_data) = address;
    return retval;
  }

  /// Common code to implement variants of the TRIM SQL function: LEADING, TRAILING, or BOTH
  static Value trimWithOptions(const std::vector<Value>& arguments, bool leading, bool trailing);

};

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
 * Returns C++ true if this Value is a boolean and is true
 * If it is NULL, return false.
 */
inline bool Value::IsTrue() const {
  if (IsBooleanNULL()) {
    return false;
  }
  return GetBoolean();
}

/**
 * Returns C++ false if this Value is a boolean and is true
 * If it is NULL, return false.
 */
inline bool Value::IsFalse() const {
  if (IsBooleanNULL()) {
    return false;
  }
  return !GetBoolean();
}

inline bool Value::IsBooleanNULL() const {
  assert(GetValueType() == VALUE_TYPE_BOOLEAN);
  return *reinterpret_cast<const int8_t*>(m_data) == INT8_NULL;
}

inline bool Value::GetSourceInlined() const {
  return m_sourceInlined;
}

inline void Value::FreeObjectsFromTupleStorage(std::vector<char*> const &oldObjects)
{

  for (std::vector<char*>::const_iterator it = oldObjects.begin(); it != oldObjects.end(); ++it) {
    Varlen* sref = reinterpret_cast<Varlen*>(*it);
    if (sref != NULL) {
      delete sref;
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
      return sizeof(char*);
    case VALUE_TYPE_DECIMAL:
      return sizeof(TTInt);
    case VALUE_TYPE_BOOLEAN:
      return sizeof(bool);
    default:
      char message[128];
      snprintf(message, 128, "Value::GetTupleStorageSize() unsupported type '%s'",
               ValueTypeToString(type).c_str());
      throw Exception(
          message);
  }
}

/**
 * This null Compare function works for GROUP BY, ORDER BY, INDEX KEY, etc,
 * except for comparison expression.
 * comparison expression has different logic for null.
 */
inline int Value::CompareNull(const Value rhs) const {
  bool lnull = IsNull();
  bool rnull = rhs.IsNull();

  if (lnull) {
    if (rnull) {
      return VALUE_COMPARE_EQUAL;
    } else {
      return VALUE_COMPARE_LESSTHAN;
    }
  } else if (rnull) {
    return VALUE_COMPARE_GREATERTHAN;
  }
  return VALUE_COMPARE_INVALID;
}

/**
 * Assuming no nulls are in comparison.
 * Compare any two Values. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of Op*.
 */
inline int Value::CompareWithoutNull(const Value rhs) const {
  assert(IsNull() == false && rhs.IsNull() == false);

  switch (m_valueType) {
    case VALUE_TYPE_VARCHAR:
      return CompareStringValue(rhs);
    case VALUE_TYPE_BIGINT:
      return CompareBigInt(rhs);
    case VALUE_TYPE_INTEGER:
      return CompareInteger(rhs);
    case VALUE_TYPE_SMALLINT:
      return CompareSmallInt(rhs);
    case VALUE_TYPE_TINYINT:
      return CompareTinyInt(rhs);
    case VALUE_TYPE_TIMESTAMP:
      return CompareTimestamp(rhs);
    case VALUE_TYPE_DOUBLE:
      return CompareDoubleValue(rhs);
    case VALUE_TYPE_VARBINARY:
      return CompareBinaryValue(rhs);
    case VALUE_TYPE_DECIMAL:
      return CompareDecimalValue(rhs);
    default: {
      throw Exception("non comparable types :: " +  GetValueTypeString() + rhs.GetValueTypeString());
    }
    /* no break */
  }
}

/**
 * Compare any two Values. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of Op*.
 */
inline int Value::Compare(const Value rhs) const {
  int hasNullCompare = CompareNull(rhs);
  if (hasNullCompare != VALUE_COMPARE_INVALID) {
    return hasNullCompare;
  }

  return CompareWithoutNull(rhs);
}

/**
 * Set this Value to null.
 */
inline void Value::SetNull() {
  tagAsNull(); // This Gets overwritten for DECIMAL -- but that's OK.
  switch (GetValueType())
  {
    case VALUE_TYPE_BOOLEAN:
      // HACK BOOL NULL
      *reinterpret_cast<int8_t*>(m_data) = INT8_NULL;
      break;
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
      *reinterpret_cast<void**>(m_data) = NULL;
      break;
    case VALUE_TYPE_DECIMAL:
      GetDecimal().SetMin();
      break;
    default: {
      throw Exception("Value::SetNull() Called with unsupported ValueType " + std::to_string(GetValueType()));
    }
  }
}


/**
 * Serialize the scalar this Value represents to the provided
 * storage area. If the scalar is an Object type that is not
 * inlined then the provided data pool or the heap will be used to
 * allocated storage for a copy of the object.
 */
inline void Value::SerializeToTupleStorageAllocateForObjects(void *storage,
                                                             const bool isInlined,
                                                             const int32_t maxLength,
                                                             const bool isInBytes,
                                                             VarlenPool *dataPool) const
{
  const ValueType type = GetValueType();

  switch (type) {
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t*>(storage) = GetTimestamp();
      break;
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(storage) = GetTinyInt();
      break;
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(storage) = GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t*>(storage) = GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(storage) = GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double*>(storage) = GetDouble();
      break;
    case VALUE_TYPE_DECIMAL:
      ::memcpy(storage, m_data, sizeof(TTInt));
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      //Potentially non-inlined type requires special handling
      if (isInlined) {
        InlineCopyyObject(storage, maxLength, isInBytes);
      }
      else {
        if (IsNull()) {
          *reinterpret_cast<void**>(storage) = NULL;
        }
        else {
          int32_t objLength = GetObjectLengthWithoutNull();
          const char* ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
          checkTooNarrowVarcharAndVarbinary(m_valueType, ptr, objLength, maxLength, isInBytes);

          const int8_t lengthLength = GetObjectLengthLength();
          const int32_t minlength = lengthLength + objLength;
          Varlen* sref = Varlen::Create(minlength, dataPool);
          char *copy = sref->Get();
          SetObjectLengthToLocation(objLength, copy);
          ::memcpy(copy + lengthLength, GetObjectValueWithoutNull(), objLength);
          *reinterpret_cast<Varlen**>(storage) = sref;
        }
      }
      break;
    default: {
      throw Exception(
          "Value::SerializeToTupleStorageAllocateForObjects() unrecognized type " +
          ValueTypeToString(type));
    }
  }
}

/**
 * Serialize the scalar this Value represents to the storage area
 * provided. If the scalar is an Object type then the object will be
 * copy if it can be inlined into the tuple. Otherwise a pointer to
 * the object will be copied into the storage area.  Any allocations
 * needed (if this Value refers to inlined memory whereas the field
 * in the tuple is not inlined), will be done in the temp string pool.
 */
inline void Value::SerializeToTupleStorage(void *storage, const bool isInlined,
                                           const int32_t maxLength, const bool isInBytes) const
{
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t*>(storage) = GetTimestamp();
      break;
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(storage) = GetTinyInt();
      break;
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(storage) = GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t*>(storage) = GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(storage) = GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double*>(storage) = GetDouble();
      break;
    case VALUE_TYPE_DECIMAL:
      ::memcpy( storage, m_data, sizeof(TTInt));
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
      //Potentially non-inlined type requires special handling
      if (isInlined) {
        InlineCopyyObject(storage, maxLength, isInBytes);
      }
      else {
        if (!IsNull()) {
          int objLength = GetObjectLengthWithoutNull();
          const char* ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
          checkTooNarrowVarcharAndVarbinary(m_valueType, ptr, objLength, maxLength, isInBytes);
        }

        // copy the Varlen pointers, even for NULL case.
        if (m_sourceInlined) {
          // Create a non-const temp here for the outlined value
          Value outlinedValue = *this;
          outlinedValue.AllocateObjectFromInlinedValue(GetTempStringPool());
          *reinterpret_cast<Varlen**>(storage) =
              *reinterpret_cast<Varlen* const*>(outlinedValue.m_data);
        }
        else {
          *reinterpret_cast<Varlen**>(storage) = *reinterpret_cast<Varlen* const*>(m_data);
        }
      }
      break;
    default:
      char message[128];
      snprintf(message, 128, "Value::SerializeToTupleStorage() unrecognized type '%s'",
               ValueTypeToString(type).c_str());
      throw Exception(message);
  }
}


/**
 * Deserialize a scalar value of the specified type from the
 * SerializeInput directly into the tuple storage area
 * provided. This function will perform memory allocations for
 * Object types as necessary using the provided data pool or the
 * heap. This is used to deserialize tables.
 */
inline void Value::DeserializeFrom(SerializeInputBE &input, VarlenPool *dataPool, char *storage,
                                   const ValueType type, bool isInlined, int32_t maxLength, bool isInBytes) {
  DeserializeFrom<TUPLE_SERIALIZATION_NATIVE>(input, dataPool, storage, type, isInlined, maxLength, isInBytes);
}

template <TupleSerializationFormat F, Endianess E> inline void Value::DeserializeFrom(SerializeInput<E> &input, VarlenPool *dataPool,
                                                                                      char *storage,
                                                                                      const ValueType type, bool isInlined,
                                                                                      int32_t maxLength, bool isInBytes) {

  switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      *reinterpret_cast<int64_t*>(storage) = input.ReadLong();
      break;
    case VALUE_TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(storage) = input.ReadByte();
      break;
    case VALUE_TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(storage) = input.ReadShort();
      break;
    case VALUE_TYPE_INTEGER:
      *reinterpret_cast<int32_t*>(storage) = input.ReadInt();
      break;
    case VALUE_TYPE_DOUBLE:
      *reinterpret_cast<double* >(storage) = input.ReadDouble();
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    {
      const int32_t length = input.ReadInt();

      const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
      // the NULL SQL string is a NULL C pointer
      if (isInlined) {
        // Always reSet the bits regardless of how long the actual value is.
        ::memset(storage, 0, lengthLength + maxLength);

        SetObjectLengthToLocation(length, storage);
        if (length == OBJECTLENGTH_NULL) {
          break;
        }
        const char *data = reinterpret_cast<const char*>(input.GetRawPointer(length));
        checkTooNarrowVarcharAndVarbinary(type, data, length, maxLength, isInBytes);

        ::memcpy( storage + lengthLength, data, length);
      } else {
        if (length == OBJECTLENGTH_NULL) {
          *reinterpret_cast<void**>(storage) = NULL;
          return;
        }
        const char *data = reinterpret_cast<const char*>(input.GetRawPointer(length));
        checkTooNarrowVarcharAndVarbinary(type, data, length, maxLength, isInBytes);

        const int32_t minlength = lengthLength + length;
        Varlen* sref = Varlen::Create(minlength, dataPool);
        char* copy = sref->Get();
        SetObjectLengthToLocation( length, copy);
        ::memcpy(copy + lengthLength, data, length);
        *reinterpret_cast<Varlen**>(storage) = sref;
      }
      break;
    }
    case VALUE_TYPE_DECIMAL: {
      if (F == TUPLE_SERIALIZATION_DR) {
        const int scale = input.ReadByte();
        const int precisionBytes = input.ReadByte();
        if (scale != kMaxDecScale) {
          throw Exception("Unexpected scale %d" + std::to_string(scale));
        }
        if (precisionBytes != 16) {
          throw Exception("Unexpected number of precision bytes %d" + std::to_string(precisionBytes));
        }
      }
      uint64_t *longStorage = reinterpret_cast<uint64_t*>(storage);
      //Reverse order for Java BigDecimal BigEndian
      longStorage[1] = input.ReadLong();
      longStorage[0] = input.ReadLong();

      if (F == TUPLE_SERIALIZATION_DR) {
        // Serialize to export serializes them in network byte order, have to reverse them here
        longStorage[0] = ntohll(longStorage[0]);
        longStorage[1] = ntohll(longStorage[1]);
      }

      break;
    }
    default:
      char message[128];
      snprintf(message, 128, "Value::DeserializeFrom() unrecognized type '%s'",
               ValueTypeToString(type).c_str());
      throw Exception(
          message);
  }
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter Sets.
 */
inline void Value::DeserializeFromAllocateForStorage(SerializeInputBE &input, VarlenPool *dataPool)
{
  const ValueType type = static_cast<ValueType>(input.ReadByte());
  DeserializeFromAllocateForStorage(type, input, dataPool);
}

inline void Value::DeserializeFromAllocateForStorage(ValueType type, SerializeInputBE &input, VarlenPool *dataPool)
{
  SetValueType(type);
  // Parameter array Value elements are reused from one executor Call to the next,
  // so these Values need to forGet they were ever null.
  m_data[13] = 0; // effectively, this is tagAsNonNull()
  switch (type) {
    case VALUE_TYPE_BIGINT:
      GetBigInt() = input.ReadLong();
      if (GetBigInt() == INT64_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_TIMESTAMP:
      GetTimestamp() = input.ReadLong();
      if (GetTimestamp() == INT64_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_TINYINT:
      GetTinyInt() = input.ReadByte();
      if (GetTinyInt() == INT8_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_SMALLINT:
      GetSmallInt() = input.ReadShort();
      if (GetSmallInt() == INT16_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_INTEGER:
      GetInteger() = input.ReadInt();
      if (GetInteger() == INT32_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_DOUBLE:
      GetDouble() = input.ReadDouble();
      if (GetDouble() <= DOUBLE_NULL) {
        tagAsNull();
      }
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    {
      const int32_t length = input.ReadInt();
      // the NULL SQL string is a NULL C pointer
      if (length == OBJECTLENGTH_NULL) {
        SetNull();
        break;
      }
      char* storage = AllocateValueStorage(length, dataPool);
      const char *str = (const char*) input.GetRawPointer(length);
      ::memcpy(storage, str, length);
      break;
    }
    case VALUE_TYPE_DECIMAL: {
      GetDecimal().table[1] = input.ReadLong();
      GetDecimal().table[0] = input.ReadLong();
      break;
    }
    case VALUE_TYPE_NULL: {
      SetNull();
      break;
    }
    case VALUE_TYPE_ARRAY: {
      DeserializeIntoANewValueList(input, dataPool);
      break;
    }
    default:
      throw Exception("Value::DeserializeFromAllocateForStorage() unrecognized type " +
                      ValueTypeToString(type));
  }
}

/**
 * Serialize this Value to the provided SerializeOutput
 */
inline void Value::SerializeTo(SerializeOutput &output) const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    {
      if (IsNull()) {
        output.WriteInt(OBJECTLENGTH_NULL);
        break;
      }
      const int32_t length = GetObjectLengthWithoutNull();
      if (length <= OBJECTLENGTH_NULL) {
        throw Exception("Attempted to serialize an Value with a negative length");
      }
      output.WriteInt(static_cast<int32_t>(length));

      // Not a null string: write it out
      output.WriteBytes(GetObjectValueWithoutNull(), length);

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
      throw Exception( "Value::SerializeTo() found a column "
          "with ValueType '%s' that is not handled" + GetValueTypeString());
  }
}

inline void Value::SerializeToExportWithoutNull(ExportSerializeOutput &io) const
{
  assert(IsNull() == false);
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    {
      io.WriteBinaryString(GetObjectValueWithoutNull(), GetObjectLengthWithoutNull());
      return;
    }
    case VALUE_TYPE_TINYINT: {
      io.WriteByte(GetTinyInt());
      return;
    }
    case VALUE_TYPE_SMALLINT: {
      io.WriteShort(GetSmallInt());
      return;
    }
    case VALUE_TYPE_INTEGER: {
      io.WriteInt(GetInteger());
      return;
    }
    case VALUE_TYPE_TIMESTAMP: {
      io.WriteLong(GetTimestamp());
      return;
    }
    case VALUE_TYPE_BIGINT: {
      io.WriteLong(GetBigInt());
      return;
    }
    case VALUE_TYPE_DOUBLE: {
      io.WriteDouble(GetDouble());
      return;
    }
    case VALUE_TYPE_DECIMAL: {
      io.WriteByte((int8_t)kMaxDecScale);
      io.WriteByte((int8_t)16);  //number of bytes in decimal
      io.WriteLong(htonll(GetDecimal().table[1]));
      io.WriteLong(htonll(GetDecimal().table[0]));
      return;
    }
    case VALUE_TYPE_INVALID:
    case VALUE_TYPE_NULL:
    case VALUE_TYPE_BOOLEAN:
    case VALUE_TYPE_ADDRESS:
    case VALUE_TYPE_ARRAY:
    case VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC:
      char message[128];
      snprintf(message, sizeof(message), "Invalid type in SerializeToExport: %s", ValueTypeToString(GetValueType()).c_str());
      throw Exception(message);
  }

  throw Exception("Invalid type in SerializeToExport");
}

/** Reformat an object-typed value from its inlined form to its
 *  allocated out-of-line form, for use with a wider/widened tuple
 *  column.  Use the pool specified by the Caller, or the temp string
 *  pool if none was supplied. **/
inline void Value::AllocateObjectFromInlinedValue(VarlenPool* pool = NULL)
{
  if (m_valueType == VALUE_TYPE_NULL || m_valueType == VALUE_TYPE_INVALID) {
    return;
  }
  assert(m_valueType == VALUE_TYPE_VARCHAR || m_valueType == VALUE_TYPE_VARBINARY);
  assert(m_sourceInlined);

  if (IsNull()) {
    *reinterpret_cast<void**>(m_data) = NULL;
    // SerializeToTupleStorage fusses about this inline flag being Set, even for NULLs
    SetSourceInlined(false);
    SetCleanUp(pool == nullptr);
    return;
  }

  if (pool == NULL) {
    pool = GetTempStringPool();
  }

  // When an object is inlined, m_data is a direct pointer into a tuple's inline storage area.
  char* source = *reinterpret_cast<char**>(m_data);

  // When it isn't inlined, m_data must contain a pointer to a Varlen object
  // that contains that same data in that same format.

  int32_t length = GetObjectLengthWithoutNull();
  // inlined objects always have a minimal (1-byte) length field.
  Varlen* sref = Varlen::Create(length + SHORT_OBJECT_LENGTHLENGTH, pool);
  char* storage = sref->Get();
  // Copy length and value into the allocated out-of-line storage
  ::memcpy(storage, source, length + SHORT_OBJECT_LENGTHLENGTH);
  SetObjectValue(sref);
  SetSourceInlined(false);
  SetCleanUp(pool == nullptr);
}

/** Deep copy an outline object-typed value from its current allocated pool,
 *  allocate the new outline object in the global temp string pool instead.
 *  The Caller needs to deallocate the original outline space for the object,
 *  probably by purging the pool that contains it.
 *  This function is used in the aggregate function for MIN/MAX functions.
 *  **/
inline void Value::AllocateObjectFromOutlinedValue()
{
  if (m_valueType == VALUE_TYPE_NULL || m_valueType == VALUE_TYPE_INVALID) {
    return;
  }
  assert(m_valueType == VALUE_TYPE_VARCHAR || m_valueType == VALUE_TYPE_VARBINARY);
  assert(!m_sourceInlined);

  if (IsNull()) {
    *reinterpret_cast<void**>(m_data) = NULL;
    return;
  }

  // Get the outline data
  const char* source = (*reinterpret_cast<Varlen* const*>(m_data))->Get();

  const int32_t length = GetObjectLengthWithoutNull() + GetObjectLengthLength();
  Varlen* sref = Varlen::Create(length, nullptr);
  char* storage = sref->Get();
  // Copy the value into the allocated out-of-line storage
  ::memcpy(storage, source, length);
  SetObjectValue(sref);
  SetSourceInlined(false);
  SetCleanUp(false);
}

inline bool Value::IsNull() const {
  if (GetValueType() == VALUE_TYPE_DECIMAL) {
    TTInt min;
    min.SetMin();
    return GetDecimal() == min;
  }
  return m_data[13] == OBJECT_NULL_BIT;
}

inline bool Value::IsNan() const {
  if (GetValueType() == VALUE_TYPE_DOUBLE) {
    return std::isnan(GetDouble());
  }
  return false;
}

// general full comparison
inline Value Value::OpEquals(const Value rhs) const {
  return Compare(rhs) == 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpNotEquals(const Value rhs) const {
  return Compare(rhs) != 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpLessThan(const Value rhs) const {
  return Compare(rhs) < 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpLessThanOrEqual(const Value rhs) const {
  return Compare(rhs) <= 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpGreaterThan(const Value rhs) const {
  return Compare(rhs) > 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpGreaterThanOrEqual(const Value rhs) const {
  return Compare(rhs) >= 0 ? GetTrue() : GetFalse();
}

// without null comparison
inline Value Value::OpEqualsWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) == 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpNotEqualsWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) != 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpLessThanWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) < 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpLessThanOrEqualWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) <= 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpGreaterThanWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) > 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpGreaterThanOrEqualWithoutNull(const Value rhs) const {
  return CompareWithoutNull(rhs) >= 0 ? GetTrue() : GetFalse();
}

inline Value Value::OpMax(const Value rhs) const {
  if (Compare(rhs) > 0) {
    return *this;
  } else {
    return rhs;
  }
}

inline Value Value::OpMin(const Value rhs) const {
  if (Compare(rhs) < 0) {
    return *this;
  } else {
    return rhs;
  }
}

inline Value Value::GetNullValue(ValueType type) {
  Value retval(type);
  retval.SetNull();
  return retval;
}

inline void Value::HashCombine(std::size_t &seed) const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_TINYINT:
      boost::hash_combine( seed, GetTinyInt()); break;
    case VALUE_TYPE_SMALLINT:
      boost::hash_combine( seed, GetSmallInt()); break;
    case VALUE_TYPE_INTEGER:
      boost::hash_combine( seed, GetInteger()); break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      boost::hash_combine( seed, GetBigInt()); break;
    case VALUE_TYPE_DOUBLE:
      // This method was observed to fail on Centos 5 / GCC 4.1.2, returning different hashes
      // for identical inputs, so the conditional was added,
      // mutated from the one in boost/type_traits/intrinsics.hpp,
      // and the broken overload for "double" was by-passed in favor of the more reliable
      // one for int64 -- even if this may give sub-optimal hashes for typical collections of double.
      // This conditional can be dropped when Centos 5 support is dropped.
#if defined(__GNUC__) && ((__GNUC__ > 4) || ((__GNUC__ == 4) && (__GNUC_MINOR__ >= 2) && !defined(__GCCXML__))) && !defined(BOOST_CLANG)
      boost::hash_combine( seed, GetDouble()); break;
#else
      {
        const int64_t proxyForDouble =  *reinterpret_cast<const int64_t*>(m_data);
        boost::hash_combine( seed, proxyForDouble); break;
      }
#endif
    case VALUE_TYPE_VARCHAR: {
      if (IsNull()) {
        boost::hash_combine( seed, std::string(""));
      } else {
        const int32_t length = GetObjectLengthWithoutNull();
        boost::hash_combine( seed, std::string( reinterpret_cast<const char*>(GetObjectValueWithoutNull()), length ));
      }
      break;
    }
    case VALUE_TYPE_VARBINARY: {
      if (IsNull()) {
        boost::hash_combine( seed, std::string(""));
      } else {
        const int32_t length = GetObjectLengthWithoutNull();
        char* data = reinterpret_cast<char*>(GetObjectValueWithoutNull());
        for (int32_t i = 0; i < length; i++)
          boost::hash_combine(seed, data[i]);
      }
      break;
    }
    case VALUE_TYPE_DECIMAL:
      GetDecimal().hash(seed); break;
    default:
      throw Exception("Value::HashCombine unknown type " +  GetValueTypeString());
  }
}

inline void* Value::CastAsAddress() const {
  const ValueType type = GetValueType();
  switch (type) {
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_ADDRESS:
      return *reinterpret_cast<void* const*>(m_data);
    default:
      throw Exception(
          "Type %s not a recognized type for casting as an address" + GetValueTypeString());
  }
}

inline Value Value::OpIncrement() const {
  const ValueType type = GetValueType();
  Value retval(type);
  switch(type) {
    case VALUE_TYPE_TINYINT:
      if (GetTinyInt() == INT8_MAX) {
        throw Exception(
            "Incrementing this TinyInt results in a value out of range");
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() + 1); break;
    case VALUE_TYPE_SMALLINT:
      if (GetSmallInt() == INT16_MAX) {
        throw Exception(
            "Incrementing this SmallInt results in a value out of range");
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() + 1); break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() == INT32_MAX) {
        throw Exception(
            "Incrementing this Integer results in a value out of range");
      }
      retval.GetInteger() = GetInteger() + 1; break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      if (GetBigInt() == INT64_MAX) {
        throw Exception(
            "Incrementing this BigInt/Timestamp results in a value out of range");
      }
      retval.GetBigInt() = GetBigInt() + 1; break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = GetDouble() + 1; break;
    default:
      throw Exception( "type %s is not incrementable " + GetValueTypeString());
      break;
  }
  return retval;
}

inline Value Value::OpDecrement() const {
  const ValueType type = GetValueType();
  Value retval(type);
  switch(type) {
    case VALUE_TYPE_TINYINT:
      if (GetTinyInt() == PELOTON_INT8_MIN) {
        throw Exception(
            "Decrementing this TinyInt results in a value out of range");
      }
      retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() - 1); break;
    case VALUE_TYPE_SMALLINT:
      if (GetSmallInt() == PELOTON_INT16_MIN) {
        throw Exception(
            "Decrementing this SmallInt results in a value out of range");
      }
      retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() - 1); break;
    case VALUE_TYPE_INTEGER:
      if (GetInteger() == PELOTON_INT32_MIN) {
        throw Exception(
            "Decrementing this Integer results in a value out of range");
      }
      retval.GetInteger() = GetInteger() - 1; break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      if (GetBigInt() == PELOTON_INT64_MIN) {
        throw Exception(
            "Decrementing this BigInt/Timestamp results in a value out of range");
      }
      retval.GetBigInt() = GetBigInt() - 1; break;
    case VALUE_TYPE_DOUBLE:
      retval.GetDouble() = GetDouble() - 1; break;
    default:
      throw Exception( "type %s is not decrementable " + GetValueTypeString());
      break;
  }
  return retval;
}

inline bool Value::IsZero() const {
  const ValueType type = GetValueType();
  switch(type) {
    case VALUE_TYPE_TINYINT:
      return GetTinyInt() == 0;
    case VALUE_TYPE_SMALLINT:
      return GetSmallInt() == 0;
    case VALUE_TYPE_INTEGER:
      return GetInteger() == 0;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return GetBigInt() == 0;
    case VALUE_TYPE_DECIMAL:
      return GetDecimal().IsZero();
    default:
      throw Exception(
          "type %s is not a numeric type that implements IsZero()" +
          GetValueTypeString());
  }
}

inline Value Value::OpSubtract(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  if (IsNull() || rhs.IsNull()) {
    return GetNullValue(vt);
  }

  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpSubtractBigInts(CastAsBigIntAndGetValue(),
                               rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpSubtractDoubles(CastAsDoubleAndGetValue(),
                               rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpSubtractDecimals(CastAsDecimal(),
                                rhs.CastAsDecimal());

    default:
      break;
  }
  throw TypeMismatchException("Promotion of %s and %s failed in OpSubtract.",
                              GetValueType(),
                              rhs.GetValueType());
}

inline Value Value::OpAdd(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  if (IsNull() || rhs.IsNull()) {
    return GetNullValue(vt);
  }

  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpAddBigInts(CastAsBigIntAndGetValue(),
                          rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpAddDoubles(CastAsDoubleAndGetValue(),
                          rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpAddDecimals(CastAsDecimal(),
                           rhs.CastAsDecimal());

    default:
      break;
  }
  throw Exception("Promotion of %s and %s failed in OpAdd." +
                  GetValueTypeString() +
                  rhs.GetValueTypeString());
}

inline Value Value::OpMultiply(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  if (IsNull() || rhs.IsNull()) {
    return GetNullValue(vt);
  }

  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpMultiplyBigInts(CastAsBigIntAndGetValue(),
                               rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpMultiplyDoubles(CastAsDoubleAndGetValue(),
                               rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpMultiplyDecimals(CastAsDecimal(),
                                rhs.CastAsDecimal());

    default:
      break;
  }
  throw Exception("Promotion of %s and %s failed in OpMultiply." +
                  GetValueTypeString() +
                  rhs.GetValueTypeString());
}

inline Value Value::OpDivide(const Value rhs) const {
  ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
  if (IsNull() || rhs.IsNull()) {
    return GetNullValue(vt);
  }

  switch (vt) {
    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      return OpDivideBigInts(CastAsBigIntAndGetValue(),
                             rhs.CastAsBigIntAndGetValue());

    case VALUE_TYPE_DOUBLE:
      return OpDivideDoubles(CastAsDoubleAndGetValue(),
                             rhs.CastAsDoubleAndGetValue());

    case VALUE_TYPE_DECIMAL:
      return OpDivideDecimals(CastAsDecimal(),
                              rhs.CastAsDecimal());

    default:
      break;
  }
  throw Exception("Promotion of %s and %s failed in OpDivide." +
                  GetValueTypeString() +
                  rhs.GetValueTypeString());
}

/*
 * Out must have storage for 16 bytes
 */
inline int32_t Value::MurmurHash3() const {
  const ValueType type = GetValueType();
  switch(type) {
    case VALUE_TYPE_TIMESTAMP:
    case VALUE_TYPE_DOUBLE:
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_TINYINT:
      return MurmurHash3_x64_128( m_data, 8, 0);
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_VARCHAR:
      if (IsNull()) {
        // Use NULL check first to be able to Get rid of checks inside of other functions.
        // Maybe it is impossible to be null here. -xin
        throw Exception("Must not ask  for object length on sql null object.");
      }
      return MurmurHash3_x64_128( GetObjectValueWithoutNull(), GetObjectLengthWithoutNull(), 0);
    default:
      throw Exception("Unknown type for murmur hashing %d" + std::to_string(type));
      break;
  }
}

/*
 * The LHS (this) should always be the string being Compared
 * and the RHS should always be the LIKE expression.
 * The planner or EE needs to enforce this.
 *
 * Null check should have been handled already.
 */
inline Value Value::Like(const Value rhs) const {
  /*
   * Validate that all params are VARCHAR
   */
  const ValueType mType = GetValueType();
  if (mType != VALUE_TYPE_VARCHAR) {
    throw Exception(
        "lhs of LIKE expression is %s not %s" +
        GetValueTypeString() +
        ValueTypeToString(VALUE_TYPE_VARCHAR));
  }

  const ValueType rhsType = rhs.GetValueType();
  if (rhsType != VALUE_TYPE_VARCHAR) {
    throw Exception(
        "rhs of LIKE expression is %s not %s" +
        rhs.GetValueTypeString() +
        ValueTypeToString(VALUE_TYPE_VARCHAR));
  }

  const int32_t valueUTF8Length = GetObjectLengthWithoutNull();
  const int32_t patternUTF8Length = rhs.GetObjectLengthWithoutNull();

  if (0 == patternUTF8Length) {
    if (0 == valueUTF8Length) {
      return GetTrue();
    } else {
      return GetFalse();
    }
  }

  char *valueChars = reinterpret_cast<char*>(GetObjectValueWithoutNull());
  char *patternChars = reinterpret_cast<char*>(rhs.GetObjectValueWithoutNull());
  assert(valueChars);
  assert(patternChars);

  /*
   * Because lambdas are for poseurs.
   */
  class Liker {

   private:
    // Constructor used internally for temporary recursion contexts.
    Liker( const Liker& original, const char* valueChars, const char* patternChars) :
      m_value(original.m_value, valueChars),
      m_pattern(original.m_pattern, patternChars)
   {}

   public:
    Liker(char *valueChars, char* patternChars, int32_t valueUTF8Length, int32_t patternUTF8Length) :
      m_value(valueChars, valueChars + valueUTF8Length),
      m_pattern(patternChars, patternChars + patternUTF8Length)
   {}

    bool Like() {
      while ( ! m_pattern.AtEnd()) {
        const uint32_t nextPatternCodePoint = m_pattern.ExtractCodePoint();
        switch (nextPatternCodePoint) {
          case '%': {
            if (m_pattern.AtEnd()) {
              return true;
            }

            const char *postPercentPatternIterator = m_pattern.GetCursor();
            const uint32_t nextPatternCodePointAfterPercent = m_pattern.ExtractCodePoint();
            const bool nextPatternCodePointAfterPercentIsSpecial =
                (nextPatternCodePointAfterPercent == '_') ||
                (nextPatternCodePointAfterPercent == '%');

            /*
             * This loop tries to skip as many characters as possible with the % by checking
             * if the next value character matches the pattern character after the %.
             *
             * If the next pattern character is special then we always have to recurse to
             * match that character. For stacked %s this just skips to the last one.
             * For stacked _ it will recurse and demand the correct number of characters.
             *
             * For a regular character it will recurse if the value character matches the pattern character.
             * This saves doing a function Call per character and allows us to skip if there is no match.
             */
            while ( ! m_value.AtEnd()) {

              const char *preExtractionValueIterator = m_value.GetCursor();
              const uint32_t nextValueCodePoint = m_value.ExtractCodePoint();

              const bool nextPatternCodePointIsSpecialOrItEqualsNextValueCodePoint =
                  (nextPatternCodePointAfterPercentIsSpecial ||
                      (nextPatternCodePointAfterPercent == nextValueCodePoint));

              if ( nextPatternCodePointIsSpecialOrItEqualsNextValueCodePoint) {
                Liker recursionContext( *this, preExtractionValueIterator, postPercentPatternIterator);
                if (recursionContext.Like()) {
                  return true;
                }
              }
            }
            return false;
          }
          case '_': {
            if ( m_value.AtEnd()) {
              return false;
            }
            //Extract a code point to consume a character
            m_value.ExtractCodePoint();
            break;
          }
          default: {
            if ( m_value.AtEnd()) {
              return false;
            }
            const uint32_t nextValueCodePoint = m_value.ExtractCodePoint();
            if (nextPatternCodePoint != nextValueCodePoint) {
              return false;
            }
            break;
          }
        }
      }
      //A matching value ends exactly where the pattern ends (having already accounted for '%')
      return m_value.AtEnd();
    }

    UTF8Iterator m_value;
    UTF8Iterator m_pattern;
  };

  Liker liker(valueChars, patternChars, valueUTF8Length, patternUTF8Length);

  return liker.Like() ? GetTrue() : GetFalse();
}

}  // End peloton namespace
