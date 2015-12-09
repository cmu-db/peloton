//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value.cpp
//
// Identification: src/backend/common/value.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/value.h"
#include "backend/common/logger.h"
#include "backend/common/stl_friendly_value.h"
#include "backend/logging/log_manager.h"
#include "backend/expression/function_expression.h"

#include <cstdio>
#include <sstream>
#include <algorithm>
#include <set>

namespace peloton {

/**
 * Public constructor that initializes to an Value that is unusable
 * with other Values.  Useful for declaring storage for an Value.
 */
Value::Value() {
  ::memset( m_data, 0, 16);
  SetValueType(VALUE_TYPE_INVALID);
  m_sourceInlined = true;
  m_cleanUp = true;
}

/**
 * Private constructor that initializes storage and the specifies the type of value
 * that will be stored in this instance
 */
Value::Value(const ValueType type) {
  ::memset( m_data, 0, 16);
  SetValueType(type);
  m_sourceInlined = true;
  m_cleanUp = true;
}


/* Objects may have storage allocated for them.
 * Release memory associated to object type Values */
Value::~Value() {

  if(m_sourceInlined == true || m_cleanUp == false)
    return;

  switch (GetValueType())
  {
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    case VALUE_TYPE_ARRAY:
    {
      Varlen* sref = *reinterpret_cast<Varlen* const*>(m_data);
      if (sref != NULL)
      {
        delete sref;
      }
    }
    break;

    default:
      return;
  }

}

Value& Value::operator=(const Value &other) {

  // protect against invalid self-assignment
  if (this != &other) {

    m_sourceInlined = other.m_sourceInlined;
    m_valueType = other.m_valueType;
    m_cleanUp = true;
    std::copy(other.m_data, other.m_data + 16, m_data);

    // Deep copy if needed
    if(m_sourceInlined == false && other.IsNull() == false) {

      switch (m_valueType) {
        case VALUE_TYPE_VARBINARY:
        case VALUE_TYPE_VARCHAR:
        case VALUE_TYPE_ARRAY:
        {
          Varlen *src_sref = *reinterpret_cast<Varlen *const *>(other.m_data);
          Varlen *new_sref = Varlen::Clone(*src_sref, nullptr);

          SetObjectValue(new_sref);
        }
        break;

        default:
          break;
      }

    }

  }

  // by convention, always return *this
  return *this;
}

Value::Value(const Value& other) {

  m_sourceInlined = other.m_sourceInlined;
  m_valueType = other.m_valueType;
  m_cleanUp = true;
  std::copy(other.m_data, other.m_data + 16, m_data);

  // Deep copy if needed
  if(m_sourceInlined == false && other.IsNull() == false) {

    switch (m_valueType) {
      case VALUE_TYPE_VARBINARY:
      case VALUE_TYPE_VARCHAR:
      case VALUE_TYPE_ARRAY:
      {
        Varlen *src_sref = *reinterpret_cast<Varlen *const *>(other.m_data);
        Varlen *new_sref = Varlen::Clone(*src_sref, nullptr);

        SetObjectValue(new_sref);
      }
      break;

      default:
        break;
    }

  }

}

Value Value::Clone(const Value &src, VarlenPool *varlen_pool __attribute__((unused))) {
  Value rv = src;

  return rv;
}

Value Value::CastAs(ValueType type) const {
  LOG_TRACE("Converting from %s to %s",
            ValueTypeToString(GetValueType()).c_str(),
            ValueTypeToString(type).c_str());
  if (GetValueType() == type) {
    return *this;
  }
  if (IsNull()) {
    return GetNullValue(type);
  }

  switch (type) {
    case VALUE_TYPE_TINYINT:
      return CastAsTinyInt();
    case VALUE_TYPE_SMALLINT:
      return CastAsSmallInt();
    case VALUE_TYPE_INTEGER:
      return CastAsInteger();
    case VALUE_TYPE_BIGINT:
      return CastAsBigInt();
    case VALUE_TYPE_TIMESTAMP:
      return CastAsTimestamp();
    case VALUE_TYPE_DOUBLE:
      return CastAsDouble();
    case VALUE_TYPE_VARCHAR:
      return CastAsString();
    case VALUE_TYPE_VARBINARY:
      return CastAsBinary();
    case VALUE_TYPE_DECIMAL:
      return CastAsDecimal();
    default:
      char message[128];
      snprintf(message, 128, "Type %d not a recognized type for casting",
               (int) type);
      throw Exception(message);
  }
}

/** Reformat an object-typed value from its inlined form to its
 *  allocated out-of-line form, for use with a wider/widened tuple
 *  column.  Use the pool specified by the Caller, or the temp string
 *  pool if none was supplied. **/
void Value::AllocateObjectFromInlinedValue(VarlenPool* pool)
{
  if (m_valueType == VALUE_TYPE_NULL || m_valueType == VALUE_TYPE_INVALID) {
    return;
  }
  assert(m_valueType == VALUE_TYPE_VARCHAR || m_valueType == VALUE_TYPE_VARBINARY);
  assert(m_sourceInlined);

  if (IsNull()) {
    *reinterpret_cast<void**>(m_data) = NULL;
    // SerializeToTupleStorage fusses about this flag being Set, even for NULLs
    SetSourceInlined(false);
    return;
  }

  // When an object is inlined, m_data is a direct pointer into a tuple's storage area.
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
  SetCleanUp(false);
}

/** Deep copy an outline object-typed value from its current allocated pool,
 *  allocate the new outline object in the global temp string pool instead.
 *  The Caller needs to deallocate the original outline space for the object,
 *  probably by purging the pool that contains it.
 *  This function is used in the aggregate function for MIN/MAX functions.
 *  **/
void Value::AllocateObjectFromOutlinedValue()
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

Value Value::GetAllocatedValue(ValueType type, const char* value, size_t size, VarlenPool* varlen_pool) {
  Value retval(type);
  char* storage = retval.AllocateValueStorage((int32_t)size, varlen_pool);
  ::memcpy(storage, value, (int32_t)size);
  retval.SetSourceInlined(false);
  retval.SetCleanUp(varlen_pool == nullptr);
  return retval;
}

char* Value::AllocateValueStorage(int32_t length, VarlenPool* varlen_pool) {
  // This unSets the Value's null tag and returns the length of the length.
  const int8_t lengthLength = SetObjectLength(length);
  const int32_t minLength = length + lengthLength;
  Varlen* sref = Varlen::Create(minLength, varlen_pool);
  char* storage = sref->Get();
  SetObjectLengthToLocation(length, storage);
  storage += lengthLength;
  SetObjectValue(sref);
  return storage;
}

/**
 * Initialize an Value of the specified type from the tuple
 * storage area provided. If this is an Object type then the third
 * argument indicates whether the object is stored in the tuple inline.
 */
Value Value::InitFromTupleStorage(const void *storage, ValueType type, bool isInlined)
{
  Value retval(type);
  switch (type)
  {
    case VALUE_TYPE_INTEGER:
      if ((retval.GetInteger() = *reinterpret_cast<const int32_t*>(storage)) == INT32_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_BIGINT:
      if ((retval.GetBigInt() = *reinterpret_cast<const int64_t*>(storage)) == INT64_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_DOUBLE:
      if ((retval.GetDouble() = *reinterpret_cast<const double*>(storage)) <= DOUBLE_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_VARCHAR:
    case VALUE_TYPE_VARBINARY:
    {
      //Potentially non-inlined type requires special handling
      if (isInlined) {
        //If it is inlined the storage area contains the actual data so copy a reference
        //to the storage area
        const char* inline_data = reinterpret_cast<const char*>(storage);
        *reinterpret_cast<const char**>(retval.m_data) = inline_data;
        retval.SetSourceInlined(true);
        /**
         * If a string is inlined in its storage location there will be no pointer to
         * check for NULL. The length preceding value must be used instead.
         */
        if ((inline_data[0] & OBJECT_NULL_BIT) != 0) {
          retval.tagAsNull();
          break;
        }
        int length = inline_data[0];
        //std::cout << "Value::InitFromTupleStorage: length: " << length << std::endl;
        retval.SetObjectLength(length); // this unSets the null tag.
        break;
      }

      // If it isn't inlined the storage area contains a pointer to the
      // Varlen object containing the string's memory
      Varlen* sref = *reinterpret_cast<Varlen**>(const_cast<void*>(storage));
      *reinterpret_cast<Varlen**>(retval.m_data) = sref;
      // If the Varlen pointer is null, that's because this
      // was a null value; otherwise Get the right char* from the Varlen
      if (sref == NULL) {
        retval.tagAsNull();
        break;
      }

      // Cache the object length in the Value.

      /* The format for a length preceding value is a 1-byte short representation
       * with the the 7th bit used to indicate a null value and the 8th bit used
       * to indicate that this is part of a long representation and that 3 bytes
       * follow. 6 bits are available to represent length for a maximum length
       * of 63 bytes representable with a single byte length. 30 bits are available
       * when the continuation bit is Set and 3 bytes follow.
       *
       * The value is converted to network byte order so that the code
       * will always know which byte contains the most signficant digits.
       */

      /*
       * Generated mask that removes the null and continuation bits
       * from a single byte length value
       */
      const char mask = ~static_cast<char>(OBJECT_NULL_BIT | OBJECT_CONTINUATION_BIT);

      char* data = sref->Get();
      int32_t length = 0;
      if ((data[0] & OBJECT_CONTINUATION_BIT) != 0) {
        char numberBytes[4];
        numberBytes[0] = static_cast<char>(data[0] & mask);
        numberBytes[1] = data[1];
        numberBytes[2] = data[2];
        numberBytes[3] = data[3];
        length = ntohl(*reinterpret_cast<int32_t*>(numberBytes));
      } else {
        length = data[0] & mask;
      }

      retval.SetObjectLength(length); // this unSets the null tag.
      retval.SetSourceInlined(false);
      retval.SetCleanUp(false);
      break;
    }
    case VALUE_TYPE_TIMESTAMP:
      if ((retval.GetTimestamp() = *reinterpret_cast<const int64_t*>(storage)) == INT64_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_TINYINT:
      if ((retval.GetTinyInt() = *reinterpret_cast<const int8_t*>(storage)) == INT8_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_SMALLINT:
      if ((retval.GetSmallInt() = *reinterpret_cast<const int16_t*>(storage)) == INT16_NULL) {
        retval.tagAsNull();
      }
      break;
    case VALUE_TYPE_DECIMAL:
    {
      ::memcpy(retval.m_data, storage, sizeof(TTInt));
      break;
    }
    default:
      throw Exception(
          "Value::InitFromTupleStorage() invalid column type " +
          ValueTypeToString(type));
      /* no break */
  }
  return retval;
}

// For x<op>y where x is an integer,
// promote x and y to s_intPromotionTable[y]
ValueType Value::s_intPromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_BIGINT,    // 3 tinyint
    VALUE_TYPE_BIGINT,    // 4 smallint
    VALUE_TYPE_BIGINT,    // 5 integer
    VALUE_TYPE_BIGINT,    // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_DOUBLE,    // 8 double
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_BIGINT,    // 11 timestamp
    // 12 - 21 unused
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_DECIMAL,   // 22 decimal
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

// for x<op>y where x is a double
// promote x and y to s_doublePromotionTable[y]
ValueType Value::s_doublePromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_DOUBLE,    // 3 tinyint
    VALUE_TYPE_DOUBLE,    // 4 smallint
    VALUE_TYPE_DOUBLE,    // 5 integer
    VALUE_TYPE_DOUBLE,    // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_DOUBLE,    // 8 double
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_DOUBLE,    // 11 timestamp
    // 12 - 21 unused.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_DOUBLE,    // 22 decimal
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

// for x<op>y where x is a decimal
// promote x and y to s_decimalPromotionTable[y]
ValueType Value::s_decimalPromotionTable[] = {
    VALUE_TYPE_INVALID,   // 0 invalid
    VALUE_TYPE_NULL,      // 1 null
    VALUE_TYPE_INVALID,   // 2 <unused>
    VALUE_TYPE_DECIMAL,   // 3 tinyint
    VALUE_TYPE_DECIMAL,   // 4 smallint
    VALUE_TYPE_DECIMAL,   // 5 integer
    VALUE_TYPE_DECIMAL,   // 6 bigint
    VALUE_TYPE_INVALID,   // 7 <unused>
    VALUE_TYPE_DOUBLE,    // 8 double
    VALUE_TYPE_INVALID,   // 9 varchar
    VALUE_TYPE_INVALID,   // 10 <unused>
    VALUE_TYPE_DECIMAL,   // 11 timestamp
    // 12 - 21 unused. ick.
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_INVALID, VALUE_TYPE_INVALID,
    VALUE_TYPE_DECIMAL,   // 22 decimal
    VALUE_TYPE_INVALID,   // 23 boolean
    VALUE_TYPE_INVALID,   // 24 address
};

TTInt Value::s_maxDecimalValue("9999999999"   //10 digits
    "9999999999"   //20 digits
    "9999999999"   //30 digits
    "99999999");    //38 digits

TTInt Value::s_minDecimalValue("-9999999999"   //10 digits
    "9999999999"   //20 digits
    "9999999999"   //30 digits
    "99999999");    //38 digits

const double Value::s_gtMaxDecimalAsDouble = 1E26;
const double Value::s_ltMinDecimalAsDouble = -1E26;

TTInt Value::s_maxInt64AsDecimal(TTInt(INT64_MAX) * kMaxScaleFactor);
TTInt Value::s_minInt64AsDecimal(TTInt(-INT64_MAX) * kMaxScaleFactor);

/*
 * Produce a debugging string describing an Value.
 */
std::string Value::Debug() const {
  const ValueType type = GetValueType();
  if (IsNull()) {
    return "<NULL>";
  }
  std::ostringstream buffer;
  std::string out_val;
  const char* ptr;
  int64_t addr;
  buffer << ValueTypeToString(type) << "::";
  switch (type) {
    case VALUE_TYPE_BOOLEAN:
      buffer << (GetBoolean() ? "true" : "false");
      break;
    case VALUE_TYPE_TINYINT:
      buffer << static_cast<int32_t>(GetTinyInt());
      break;
    case VALUE_TYPE_SMALLINT:
      buffer << GetSmallInt();
      break;
    case VALUE_TYPE_INTEGER:
      buffer << GetInteger();
      break;
    case VALUE_TYPE_BIGINT:
    case VALUE_TYPE_TIMESTAMP:
      buffer << GetBigInt();
      break;
    case VALUE_TYPE_DOUBLE:
      buffer << GetDouble();
      break;
    case VALUE_TYPE_VARCHAR:
      ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
      addr = reinterpret_cast<int64_t>(ptr);
      out_val = std::string(ptr, GetObjectLengthWithoutNull());
      buffer << "[" << GetObjectLengthWithoutNull() << "]";
      buffer << "\"" << out_val << "\"[@" << addr << "]";
      break;
    case VALUE_TYPE_VARBINARY:
      ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
      addr = reinterpret_cast<int64_t>(ptr);
      out_val = std::string(ptr, GetObjectLengthWithoutNull());
      buffer << "[" << GetObjectLengthWithoutNull() << "]";
      buffer << "-bin[@" << addr << "]";
      break;
    case VALUE_TYPE_DECIMAL:
      buffer << CreateStringFromDecimal();
      break;
    default:
      buffer << "(no details)";
      break;
  }
  std::string ret(buffer.str());
  return (ret);
}


/**
 * Serialize sign and value using radix point (no exponent).
 */
std::string Value::CreateStringFromDecimal() const {
  assert(!IsNull());
  std::ostringstream buffer;
  TTInt scaledValue = GetDecimal();
  if (scaledValue.IsSign()) {
    buffer << '-';
  }
  TTInt whole(scaledValue);
  TTInt fractional(scaledValue);
  whole /= Value::kMaxScaleFactor;
  fractional %= Value::kMaxScaleFactor;
  if (whole.IsSign()) {
    whole.ChangeSign();
  }
  buffer << whole.ToString(10);
  buffer << '.';
  if (fractional.IsSign()) {
    fractional.ChangeSign();
  }
  std::string fractionalString = fractional.ToString(10);
  for (int ii = static_cast<int>(fractionalString.size()); ii < Value::kMaxDecScale; ii++) {
    buffer << '0';
  }
  buffer << fractionalString;
  return buffer.str();
}

/**
 *   Set a decimal value from a serialized representation
 *   This function does not handle scientific notation string, Java planner should convert that to plan string first.
 */
void Value::CreateDecimalFromString(const std::string &txt) {
  if (txt.length() == 0) {
    throw SerializationException("Empty string provided");
  }
  bool setSign = false;
  if (txt[0] == '-') {
    setSign = true;
  }

  /**
   * Check for invalid characters
   */
  for (int ii = (setSign ? 1 : 0); ii < static_cast<int>(txt.size()); ii++) {
    if ((txt[ii] < '0' || txt[ii] > '9') && txt[ii] != '.') {
      char message[4096];
      snprintf(message, 4096, "Invalid characters in decimal string: %s",
               txt.c_str());
      throw SerializationException(message);
    }
  }

  std::size_t separatorPos = txt.find( '.', 0);
  if (separatorPos == std::string::npos) {
    const std::string wholeString = txt.substr( setSign ? 1 : 0, txt.size());
    const std::size_t wholeStringSize = wholeString.size();
    if (wholeStringSize > 26) {
      throw SerializationException("Maximum precision exceeded. Maximum of 26 digits to the left of the decimal point");
    }
    TTInt whole(wholeString);
    if (setSign) {
      whole.SetSign();
    }
    whole *= kMaxScaleFactor;
    GetDecimal() = whole;
    return;
  }

  if (txt.find( '.', separatorPos + 1) != std::string::npos) {
    throw SerializationException("Too many decimal points");
  }

  // This is set to 1 if we carry in the scale.
  int carryScale = 0;
  // This is set to 1 if we carry from the scale to the whole.
  int carryWhole = 0;

  // Start with the fractional part.  We need to
  // see if we need to carry from it first.
  std::string fractionalString = txt.substr( separatorPos + 1, txt.size() - (separatorPos + 1));
  // remove trailing zeros
  while (fractionalString.size() > 0 && fractionalString[fractionalString.size() - 1] == '0')
    fractionalString.erase(fractionalString.size() - 1, 1);
  //
  // If the scale is too large, then we will round
  // the number to the nearest 10**-12, and to the
  // furthest from zero if the number is equidistant
  // from the next highest and lowest.  This is the
  // definition of the Java rounding mode HALF_UP.
  //
  // At some point we will read a rounding mode from the
  // Java side at Engine configuration time, or something
  // like that, and have a whole flurry of rounding modes
  // here.  However, for now we have just the one.
  //
  if (fractionalString.size() > kMaxDecScale) {
    carryScale = ('5' <= fractionalString[kMaxDecScale]) ? 1 : 0;
    fractionalString = fractionalString.substr(0, kMaxDecScale);
  } else {
    while(fractionalString.size() < Value::kMaxDecScale) {
      fractionalString.push_back('0');
    }
  }
  TTInt fractional(fractionalString);

  // If we decided to carry above, then do it here.
  // The fractional string is set up so that it represents
  // 1.0e-12 * units.
  fractional += carryScale;
  if (TTInt((uint64_t)kMaxScaleFactor) <= fractional) {
    // We know fractional was < kMaxScaleFactor before
    // we rounded, since fractional is 12 digits and
    // kMaxScaleFactor is 13.  So, if carrying makes
    // the fractional number too big, it must be eactly
    // too big.  That is to say, the rounded fractional
    // number number has become zero, and we need to
    // carry to the whole number.
    fractional = 0;
    carryWhole = 1;
  }

  // Process the whole number string.
  const std::string wholeString = txt.substr( setSign ? 1 : 0, separatorPos - (setSign ? 1 : 0));
  // We will check for oversize numbers below, so don't waste time
  // doing it now.
  TTInt whole(wholeString);
  whole += carryWhole;
  if (oversizeWholeDecimal(whole)) {
    throw SerializationException("Maximum precision exceeded. Maximum of 26 digits to the left of the decimal point");
  }
  whole *= kMaxScaleFactor;
  whole += fractional;

  if (setSign) {
    whole.SetSign();
  }

  GetDecimal() = whole;
}

struct ValueList {
  static int AllocationSizeForLength(size_t length)
  {
    //TODO: May want to consider extra allocation, here,
    // such as space for a sorted copy of the array.
    // This allocation has the advantage of Getting freed via Value::free.
    return (int)(sizeof(ValueList) + length*sizeof(StlFriendlyValue));
  }

  void* operator new(__attribute__((unused)) size_t size, char* placement)
  {
    return placement;
  }
  void operator delete(void*, char*) {}
  void operator delete(void*) {}

  ValueList(size_t length, ValueType elementType) : m_length(length), m_elementType(elementType)
  { }

  void DeserializeValues(SerializeInputBE &input, VarlenPool *varlen_pool)
  {
    for (size_t ii = 0; ii < m_length; ++ii) {
      m_values[ii].DeserializeFromAllocateForStorage(m_elementType, input, varlen_pool);
    }
  }

  StlFriendlyValue const* begin() const { return m_values; }
  StlFriendlyValue const* end() const { return m_values + m_length; }

  const size_t m_length;
  const ValueType m_elementType;
  StlFriendlyValue m_values[0];
};

/**
 * This Value can be of any scalar value type.
 * @param rhs  a VALUE_TYPE_ARRAY Value whose referent must be an ValueList.
 *             The Value elements of the ValueList should be comparable to and ideally
 *             of exactly the same VALUE_TYPE as "this".
 * The planner and/or Deserializer should have taken care of this with checks and
 * explicit cast operators and and/or constant promotions as needed.
 * @return a VALUE_TYPE_BOOLEAN Value.
 */
bool Value::InList(const Value& rhs) const
{
  //TODO: research: does the SQL standard allow a null to match a null list element
  // vs. returning FALSE or NULL?
  const bool lhsIsNull = IsNull();
  if (lhsIsNull) {
    return false;
  }

  const ValueType rhsType = rhs.GetValueType();
  if (rhsType != VALUE_TYPE_ARRAY) {
    throw Exception("rhs of IN expression is of a non-list type " + rhs.GetValueTypeString());
  }
  const ValueList* listOfValues = (ValueList*)rhs.GetObjectValueWithoutNull();
  const StlFriendlyValue& value = *static_cast<const StlFriendlyValue*>(this);
  //TODO: An O(ln(length)) implementation vs. the current O(length) implementation
  // such as binary search would likely require some kind of sorting/re-org of values
  // post-update/pre-lookup, and would likely require some sortable inequality method
  // (operator<()?) to be defined on StlFriendlyValue.
  return std::find(listOfValues->begin(), listOfValues->end(), value) != listOfValues->end();
}

void Value::DeserializeIntoANewValueList(SerializeInputBE &input, VarlenPool *varlen_pool)
{
  ValueType elementType = (ValueType)input.ReadByte();
  size_t length = input.ReadShort();
  int trueSize = ValueList::AllocationSizeForLength(length);
  char* storage = AllocateValueStorage(trueSize, varlen_pool);
  ::memset(storage, 0, trueSize);
  ValueList* nvset = new (storage) ValueList(length, elementType);
  nvset->DeserializeValues(input, varlen_pool);
  //TODO: An O(ln(length)) implementation vs. the current O(length) implementation of Value::inList
  // would likely require some kind of sorting/re-org of values at this point post-update pre-lookup.
}

void Value::AllocateANewValueList(size_t length, ValueType elementType)
{
  int trueSize = ValueList::AllocationSizeForLength(length);
  char* storage = AllocateValueStorage(trueSize, nullptr);
  ::memset(storage, 0, trueSize);
  new (storage) ValueList(length, elementType);
}

void Value::SetArrayElements(std::vector<Value> &args) const
{
  assert(m_valueType == VALUE_TYPE_ARRAY);
  ValueList* listOfValues = (ValueList*)GetObjectValue();
  // Assign each of the elements.
  size_t ii = args.size();
  assert(ii == listOfValues->m_length);
  while (ii--) {
    listOfValues->m_values[ii] = args[ii];
  }
  //TODO: An O(ln(length)) implementation vs. the current O(length) implementation of Value::inList
  // would likely require some kind of sorting/re-org of values at this point post-update pre-lookup.
}

int Value::ArrayLength() const
{
  assert(m_valueType == VALUE_TYPE_ARRAY);
  ValueList* listOfValues = (ValueList*)GetObjectValue();
  return static_cast<int>(listOfValues->m_length);
}

Value Value::ItemAtIndex(int index) const
{
  assert(m_valueType == VALUE_TYPE_ARRAY);
  ValueList* listOfValues = (ValueList*)GetObjectValue();
  assert(index >= 0);
  assert(index < (int) listOfValues->m_length);
  return listOfValues->m_values[index];
}

void Value::CastAndSortAndDedupArrayForInList(const ValueType outputType, std::vector<Value> &outList) const
{
  int size = ArrayLength();

  // make a set to eliminate unique values in O(nlogn) time
  std::set<StlFriendlyValue> uniques;

  // iterate over the array of values and build a sorted set of unique
  // values that don't overflow or violate unique constaints
  // (n.b. sorted set means dups are removed)
  for (int i = 0; i < size; i++) {
    Value value = ItemAtIndex(i);
    // cast the value to the right type and catch overflow/cast problems
    try {
      StlFriendlyValue stlValue;
      stlValue = value.CastAs(outputType);
      std::pair<std::set<StlFriendlyValue>::iterator, bool> ret;
      ret = uniques.insert(stlValue);
    }
    // cast exceptions mean the in-list test is redundant
    // don't include these values in the materialized table
    // TODO: make this less hacky
    catch (Exception &sqlException) {}
  }

  // insert all items in the set in order
  std::set<StlFriendlyValue>::const_iterator iter;
  for (iter = uniques.begin(); iter != uniques.end(); iter++) {
    outList.push_back(*iter);
  }
}

void Value::streamTimestamp(std::stringstream& value) const
{
  int64_t epoch_micros = GetTimestamp();
  boost::gregorian::date as_date;
  boost::posix_time::time_duration as_time;
  micros_to_date_and_time(epoch_micros, as_date, as_time);

  long micro = epoch_micros % 1000000;
  if (epoch_micros < 0 && micro < 0) {
    // deal with negative micros (for dates before 1970)
    // by taking back the 1 whole second that was rounded down from the formatted date/time
    // and converting it to 1000000 micros
    micro += 1000000;
  }
  char mbstr[27];    // Format: "YYYY-MM-DD HH:MM:SS."- 20 characters + terminator
  snprintf(mbstr, sizeof(mbstr), "%04d-%02d-%02d %02d:%02d:%02d.%06d",
           (int)as_date.year(), (int)as_date.month(), (int)as_date.day(),
           (int)as_time.hours(), (int)as_time.minutes(), (int)as_time.seconds(), (int)micro);
  value << mbstr;
}

static void throwTimestampFormatError(const std::string &str)
{
  char message[4096];
  // No space separator for between the date and time
  snprintf(message, 4096, "Attempted to cast \'%s\' to type %s failed. Supported format: \'YYYY-MM-DD HH:MM:SS.UUUUUU\'"
           "or \'YYYY-MM-DD\'",
           str.c_str(), ValueTypeToString(VALUE_TYPE_TIMESTAMP).c_str());
  throw Exception(message);
}

int64_t Value::parseTimestampString(const std::string &str)
{
  // date_str
  std::string date_str(str);
  // This is the std:string API for "ltrim" and "rtrim".
  date_str.erase(date_str.begin(), std::find_if(date_str.begin(), date_str.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
  date_str.erase(std::find_if(date_str.rbegin(), date_str.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), date_str.end());
  std::size_t sep_pos;


  int year = 0;
  int month = 0;
  int day = 0;
  int hour = 0;
  int minute = 0;
  int second = 0;
  int micro = 1000000;
  // time_str
  std::string time_str;
  std::string number_string;
  const char * pch;

  switch (date_str.size()) {
    case 26:
      sep_pos  = date_str.find(' ');
      if (sep_pos != 10) {
        throwTimestampFormatError(str);
      }

      time_str = date_str.substr(sep_pos + 1);
      // This is the std:string API for "ltrim"
      time_str.erase(time_str.begin(), std::find_if(time_str.begin(), time_str.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
      if (time_str.length() != 15) {
        throwTimestampFormatError(str);
      }

      // tokenize time_str: HH:MM:SS.mmmmmm
      if (time_str.at(2) != ':' || time_str.at(5) != ':' || time_str.at(8) != '.') {
        throwTimestampFormatError(str);
      }

      // HH
      number_string = time_str.substr(0,2);
      pch = number_string.c_str();
      if (pch[0] == '0') {
        hour = 0;
      } else if (pch[0] == '1') {
        hour = 10;
      } else if (pch[0] == '2') {
        hour = 20;
      } else {
        throwTimestampFormatError(str);
      }
      if (pch[1] > '9' || pch[1] < '0') {
        throwTimestampFormatError(str);
      }
      hour += pch[1] - '0';
      if (hour > 23 || hour < 0) {
        throwTimestampFormatError(str);
      }

      // MM
      number_string = time_str.substr(3,2);
      pch = number_string.c_str();
      if (pch[0] > '5' || pch[0] < '0') {
        throwTimestampFormatError(str);
      }
      minute = 10*(pch[0] - '0');
      if (pch[1] > '9' || pch[1] < '0') {
        throwTimestampFormatError(str);
      }
      minute += pch[1] - '0';
      if (minute > 59 || minute < 0) {
        throwTimestampFormatError(str);
      }

      // SS
      number_string = time_str.substr(6,2);
      pch = number_string.c_str();
      if (pch[0] > '5' || pch[0] < '0') {
        throwTimestampFormatError(str);
      }
      second = 10*(pch[0] - '0');
      if (pch[1] > '9' || pch[1] < '0') {
        throwTimestampFormatError(str);
      }
      second += pch[1] - '0';
      if (second > 59 || second < 0) {
        throwTimestampFormatError(str);
      }
      // hack a '1' in the place if the decimal and use atoi to Get a value that
      // MUST be between 1 and 2 million if all 6 digits of micros were included.
      number_string = time_str.substr(8,7);
      number_string.at(0) = '1';
      pch = number_string.c_str();
      micro = atoi(pch);
      if (micro >= 2000000 || micro < 1000000) {
        throwTimestampFormatError(str);
      }
    case 10:
      if (date_str.at(4) != '-' || date_str.at(7) != '-') {
        throwTimestampFormatError(str);
      }

      number_string = date_str.substr(0,4);
      pch = number_string.c_str();

      // YYYY
      year = atoi(pch);
      // new years day 10000 is likely to cause problems.
      // There's a boost library limitation against years before 1400.
      if (year > 9999 || year < 1400) {
        throwTimestampFormatError(str);
      }

      // MM
      number_string = date_str.substr(5,2);
      pch = number_string.c_str();
      if (pch[0] == '0') {
        month = 0;
      } else if (pch[0] == '1') {
        month = 10;
      } else {
        throwTimestampFormatError(str);
      }
      if (pch[1] > '9' || pch[1] < '0') {
        throwTimestampFormatError(str);
      }
      month += pch[1] - '0';
      if (month > 12 || month < 1) {
        throwTimestampFormatError(str);
      }

      // DD
      number_string = date_str.substr(8,2);
      pch = number_string.c_str();
      if (pch[0] == '0') {
        day = 0;
      } else if (pch[0] == '1') {
        day = 10;
      } else if (pch[0] == '2') {
        day = 20;
      } else if (pch[0] == '3') {
        day = 30;
      } else {
        throwTimestampFormatError(str);
      }
      if (pch[1] > '9' || pch[1] < '0') {
        throwTimestampFormatError(str);
      }
      day += pch[1] - '0';
      if (day > 31 || day < 1) {
        throwTimestampFormatError(str);
      }
      break;
    default:
      throwTimestampFormatError(str);
  }

  int64_t result = 0;
  try {
    result = epoch_microseconds_from_components(
        (unsigned short int)year, (unsigned short int)month, (unsigned short int)day,
        hour, minute, second);
  } catch (const std::out_of_range& bad) {
    throwTimestampFormatError(str);
  }
  result += (micro - 1000000);
  return result;
}


int WarnIf(int condition, __attribute__((unused)) const char* message)
{
  if (condition) {
    //LogManager::GetThreadLogger(LOGGERID_HOST)->log(LOGLEVEL_WARN, message);
  }
  return condition;
}

// Get a string representation of this value
std::ostream &operator<<(std::ostream &os, const Value &value) {
  os << value.Debug();
  return os;
}

Value Value::GetMinValue(ValueType type) {
  switch (type) {
    case (VALUE_TYPE_TINYINT):
                      return GetTinyIntValue(PELOTON_INT8_MIN);
    break;
    case (VALUE_TYPE_SMALLINT):
                      return GetSmallIntValue(PELOTON_INT16_MIN);
    break;
    case (VALUE_TYPE_INTEGER):
                      return GetIntegerValue(PELOTON_INT32_MIN);
    break;
    break;
    case (VALUE_TYPE_BIGINT):
                      return GetBigIntValue(PELOTON_INT64_MIN);
    break;
    case (VALUE_TYPE_DOUBLE):
                      return GetDoubleValue(-DBL_MAX);
    break;
    case (VALUE_TYPE_VARCHAR):
                      return GetTempStringValue("", 2);
    break;
    case (VALUE_TYPE_TIMESTAMP):
                      return GetTimestampValue(PELOTON_INT64_MIN);
    break;
    case (VALUE_TYPE_DECIMAL):
                      return GetDecimalValue(DECIMAL_MIN);
    break;
    case (VALUE_TYPE_BOOLEAN):
                      return GetFalse();
    break;

    case (VALUE_TYPE_INVALID):
    case (VALUE_TYPE_NULL):
    case (VALUE_TYPE_ADDRESS):
    case (VALUE_TYPE_VARBINARY):
    default: {
      throw UnknownTypeException((int)type, "Can't get min value for type");
    }
  }
}


}  // End peloton namespace
