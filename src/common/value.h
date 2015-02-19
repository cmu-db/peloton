/*-------------------------------------------------------------------------
 *
 * value.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/value.h
 *
 *-------------------------------------------------------------------------
 */

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

#include "ttmath/ttmathint.h"
#include "boost/scoped_ptr.hpp"
#include "boost/unordered_map.hpp"

#include "common/exception.h"
#include "common/types.h"
#include "common/serializer.h"
#include "common/varlen.h"
#include "common/varlen_pool.h"
#include "common/export_serializer.h"

namespace nstore {

#define CHECK_FPE( x ) ( std::isinf(x) || std::isnan(x) )

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

//The int used for storage and return values
typedef ttmath::Int<2> TTInt;

//Long integer with space for multiplication and division without carry/overflow
typedef ttmath::Int<4> TTLInt;

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

public:
	/* Create a default Value */
	Value();

	// todo: free() should not really be const
	/* Release memory associated to object type Values */

	void free() const;

	/* Set value to the correct SQL NULL representation. */
	void setNull();

	/* Reveal the contained pointer for type values */
	void* castAsAddress() const;

	/* Create a boolean true Value */
	static Value getTrue();

	/* Create a boolean false Value */
	static Value getFalse();

	/* Create an Value with the null representation for valueType */
	static Value getNullValue(ValueType);

	/* Create an Value promoted/demoted to type */
	Value castAs(ValueType type) const;

	// todo: Why doesn't this return size_t? Also, this is a
	// quality of ValueType, not Value.
	/* Calculate the tuple storage size for an Value type. VARCHARs assume out-of-band tuple storage */
	static uint16_t getTupleStorageSize(const ValueType type);

	// todo: Could the isInlined argument be removed by have the
	// caller dereference the pointer?
	/* Deserialize a scalar of the specified type from the tuple
	 * storage area provided. If this is an Object type then the third
	 * argument indicates whether the object is stored in the tuple inline */
	static const Value deserializeFromTupleStorage(
			const void *storage, const ValueType type, const bool isInlined);

	/* Serialize the scalar this Value represents to the provided
	 * storage area. If the scalar is an Object type that is not
	 * inlined then the provided data pool or the heap will be used to
	 * allocated storage for a copy of the object. */
	void serializeToTupleStorageAllocateForObjects(
			void *storage, const bool isInlined, const int32_t maxLength,
			Pool *dataPool) const;

	/* Serialize the scalar this Value represents to the storage area
	 * provided. If the scalar is an Object type then the object will
	 * be copy if it can be inlined into the tuple. Otherwise a
	 * pointer to the object will be copied into the storage area. No
	 * allocations are performed. */
	void serializeToTupleStorage(
			void *storage, const bool isInlined, const int32_t maxLength) const;

	/* Deserialize a scalar value of the specified type from the
	 * SerializeInput directly into the tuple storage area
	 * provided. This function will perform memory allocations for
	 * Object types as necessary using the provided data pool or the
	 * heap. This is used to deserialize tables. */
	static int64_t deserializeFrom(
			SerializeInput &input, const ValueType type, char *storage,
			bool isInlined, const int32_t maxLength, Pool *dataPool);

	// TODO: no callers use the first form; Should combine these
	// eliminate the potential Value copy.
	/* Read a ValueType from the SerializeInput stream and deserialize
	 * a scalar value of the specified type from the provided
	 * SerializeInput and perform allocations as necessary. */
	static const Value deserializeFromAllocateForStorage(
			SerializeInput &input, Pool *dataPool);

	/* Serialize this Value to a SerializeOutput */
	void serializeTo(SerializeOutput &output) const;

	/* Serialize this Value to an Export stream */
	void serializeToExport(ExportSerializeOutput&) const;

	/* Check if the value represents SQL NULL */
	bool isNull() const;

	/* For boolean Values, convert to bool */
	bool isTrue() const;
	bool isFalse() const;

	/* For number values, check the number line. */
	bool isNegative() const;
	bool isZero() const;

	/* For boolean Values only, logical operators */
	Value op_negate() const;
	Value op_and(const Value rhs) const;
	Value op_or(const Value rhs) const;

	/* Evaluate the ordering relation against two Values. Promotes
       exact types to allow disparate type comparison. See also the
       op_ functions which return boolean Values. */
	int compare(const Value rhs) const;

	/* Return a boolean Value with the comparison result */
	Value op_equals(const Value rhs) const;
	Value op_notEquals(const Value rhs) const;
	Value op_lessThan(const Value rhs) const;
	Value op_lessThanOrEqual(const Value rhs) const;
	Value op_greaterThan(const Value rhs) const;
	Value op_greaterThanOrEqual(const Value rhs) const;

	/* Return a copy of MAX(this, rhs) */
	Value op_max(const Value rhs) const;

	/* Return a copy of MIN(this, rhs) */
	Value op_min(const Value rhs) const;

	/* For number Values, compute new Values for arthimetic operators */
	Value op_increment() const;
	Value op_decrement() const;
	Value op_subtract(const Value rhs) const;
	Value op_add(const Value rhs) const;
	Value op_multiply(const Value rhs) const;
	Value op_divide(const Value rhs) const;

	/* For boost hashing */
	void hashCombine(std::size_t &seed) const;

	/* Functor comparator for use with std::set */
	struct ltValue {
		bool operator()(const Value v1, const Value v2) const {
			return v1.compare(v2) < 0;
		}
	};

	/* Functor equality predicate for use with boost unordered */
	struct equal_to : std::binary_function<Value, Value, bool>
	{
		bool operator()(Value const& x,
				Value const& y) const
		{
			return x.compare(y) == 0;
		}
	};

	/* Functor hash predicate for use with boost unordered */
	struct hash : std::unary_function<Value, std::size_t>
	{
		std::size_t operator()(Value const& x) const
		{
			std::size_t seed = 0;
			x.hashCombine(seed);
			return seed;
		}
	};

	/* Return a string full of arcana and wonder. */
	std::string debug() const;

	// Constants for Decimal type
	// Precision and scale (inherent in the schema)
	static const uint16_t kMaxDecPrec = 38;
	static const uint16_t kMaxDecScale = 12;
	static const int64_t kMaxScaleFactor = 1000000000000;

	const int32_t& getInteger() const {
		assert(getValueType() == VALUE_TYPE_INTEGER);
		return *reinterpret_cast<const int32_t*>(m_data);
	}

private:
	// Function declarations for Value.cpp definitions.

	static std::string getTypeName(ValueType type);
	void createDecimalFromString(const std::string &txt);
	std::string createStringFromDecimal() const;
	Value opDivideDecimals(const Value lhs, const Value rhs) const;
	Value opMultiplyDecimals(const Value &lhs, const Value &rhs) const;

	// Promotion Rules. Initialized in Value.cpp
	static ValueType s_intPromotionTable[];
	static ValueType s_decimalPromotionTable[];
	static ValueType s_doublePromotionTable[];
	static TTInt s_maxDecimal;
	static TTInt s_minDecimal;

	static ValueType promoteForOp(ValueType vta, ValueType vtb) {
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
		assert(rt != VALUE_TYPE_INVALID);
		return rt;
	}

	/**
	 * 16 bytes of storage for Value data.
	 */
	char m_data[16];
	ValueType m_valueType;
	bool m_sourceInlined;

	/**
	 * Private constructor that initializes storage and the specifies the type of value
	 * that will be stored in this instance
	 */
	Value(const ValueType type) {
		::memset( m_data, 0, 16);
		setValueType(type);
		m_sourceInlined = false;
	}

	/**
	 * Set the type of the value that will be stored in this instance.
	 * The last of the 16 bytes of storage allocated in an Value
	 * is used to store the type
	 */
	void setValueType(ValueType type) {
		m_valueType = type;
	}

	/**
	 * Get the type of the value. This information is private
	 * to prevent code outside of Value from branching based on the type of a value.
	 */
	ValueType getValueType() const {
		return m_valueType;
	}

	void setSourceInlined(bool sourceInlined)
	{
		m_sourceInlined = sourceInlined;
	}

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
	int32_t getObjectLength() const {
		if (isNull()) {
			// Conceptually, I think a NULL object should just have
			// length 0. In practice, this code path is often a defect
			// in code not correctly handling null. May favor a more
			// defensive "return 0" in the future? (rtb)
			throw Exception("Must not ask  for object length on sql null object.");
		}
		if ((getValueType() != VALUE_TYPE_VARCHAR) && (getValueType() != VALUE_TYPE_VARBINARY)) {
			// probably want getTupleStorageSize() for non-object types.
			// at the moment, only varchars are using getObjectLength().
			throw Exception("Must not ask for object length for non-object types");
		}

		// now safe to read and return the length preceding value.
		return *reinterpret_cast<const int32_t *>(&m_data[8]);
	}

	void setObjectLength(int32_t length) {
		*reinterpret_cast<int32_t *>(&m_data[8]) = length;
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
	static int32_t getObjectLengthFromLocation(const char *location) {
		/*
		 * Location will be NULL if the Value is operating on storage that is not
		 * inlined and thus can contain a NULL pointer
		 */
		if (location == NULL) {
			return -1;
		}
		char firstByte = location[0];
		const char premask = static_cast<char>(OBJECT_NULL_BIT | OBJECT_CONTINUATION_BIT);

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
			decodedNumber = ntohl(*reinterpret_cast<int32_t*>(numberBytes));
		} else {
			decodedNumber = location[0] & mask;
		}
		return decodedNumber;
	}

	/*
	 * Retrieve the number of bytes used by the length preceding value
	 * in the object's storage area. This value
	 * is cached in the Value's 13th byte.
	 */
	int8_t getObjectLengthLength() const {
		return m_data[12];
	}

	/*
	 * Set the objects length preceding values length to
	 * the specified value
	 */
	void setObjectLengthLength(int8_t length) {
		m_data[12] = length;
	}

	/*
	 * Based on the objects actual length value get the length of the
	 * length preceding value to the appropriate length
	 */
	static int8_t getAppropriateObjectLengthLength(int32_t length) {
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
	static void setObjectLengthToLocation(int32_t length, char *location) {
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
	 * Not truly symmetrical with getObjectValue which returns the actual object past
	 * the length preceding value
	 */
	void setObjectValue(void* object) {
		*reinterpret_cast<void**>(m_data) = object;
	}

	/**
	 * Get a pointer to the value of an Object that lies beyond the storage of the length information
	 */
	void* getObjectValue() const {
		if (*reinterpret_cast<void* const*>(m_data) == NULL) {
			return NULL;
		} else if(*reinterpret_cast<const int32_t*>(&m_data[8]) == OBJECTLENGTH_NULL) {
			return NULL;
		} else {
			void* value;
			if (m_sourceInlined)
			{
				value = *reinterpret_cast<char* const*>(m_data) + getObjectLengthLength();
			}
			else
			{
				StringRef* sref = *reinterpret_cast<StringRef* const*>(m_data);
				value = sref->get() + getObjectLengthLength();
			}
			return value;
		}
	}

	// getters
	const int8_t& getTinyInt() const {
		assert(getValueType() == VALUE_TYPE_TINYINT);
		return *reinterpret_cast<const int8_t*>(m_data);
	}

	int8_t& getTinyInt() {
		assert(getValueType() == VALUE_TYPE_TINYINT);
		return *reinterpret_cast<int8_t*>(m_data);
	}

	const int16_t& getSmallInt() const {
		assert(getValueType() == VALUE_TYPE_SMALLINT);
		return *reinterpret_cast<const int16_t*>(m_data);
	}

	int16_t& getSmallInt() {
		assert(getValueType() == VALUE_TYPE_SMALLINT);
		return *reinterpret_cast<int16_t*>(m_data);
	}

	int32_t& getInteger() {
		assert(getValueType() == VALUE_TYPE_INTEGER);
		return *reinterpret_cast<int32_t*>(m_data);
	}

	const int64_t& getBigInt() const {
		assert((getValueType() == VALUE_TYPE_BIGINT) ||
				(getValueType() == VALUE_TYPE_TIMESTAMP) ||
				(getValueType() == VALUE_TYPE_ADDRESS));
		return *reinterpret_cast<const int64_t*>(m_data);
	}

	int64_t& getBigInt() {
		assert((getValueType() == VALUE_TYPE_BIGINT) ||
				(getValueType() == VALUE_TYPE_TIMESTAMP) ||
				(getValueType() == VALUE_TYPE_ADDRESS));
		return *reinterpret_cast<int64_t*>(m_data);
	}

	const int64_t& getTimestamp() const {
		assert(getValueType() == VALUE_TYPE_TIMESTAMP);
		return *reinterpret_cast<const int64_t*>(m_data);
	}

	int64_t& getTimestamp() {
		assert(getValueType() == VALUE_TYPE_TIMESTAMP);
		return *reinterpret_cast<int64_t*>(m_data);
	}

	const double& getDouble() const {
		assert(getValueType() == VALUE_TYPE_DOUBLE);
		return *reinterpret_cast<const double*>(m_data);
	}

	double& getDouble() {
		assert(getValueType() == VALUE_TYPE_DOUBLE);
		return *reinterpret_cast<double*>(m_data);
	}

	const bool& getBoolean() const {
		assert(getValueType() == VALUE_TYPE_BOOLEAN);
		return *reinterpret_cast<const bool*>(m_data);
	}

	TTInt& getDecimal() {
		assert(getValueType() == VALUE_TYPE_DECIMAL);
		void* retval = reinterpret_cast<void*>(m_data);
		return *reinterpret_cast<TTInt*>(retval);
	}

	const TTInt& getDecimal() const {
		assert(getValueType() == VALUE_TYPE_DECIMAL);
		const void* retval = reinterpret_cast<const void*>(m_data);
		return *reinterpret_cast<const TTInt*>(retval);
	}

	bool& getBoolean() {
		assert(getValueType() == VALUE_TYPE_BOOLEAN);
		return *reinterpret_cast<bool*>(m_data);
	}

	std::size_t getAllocationSizeForObject() const;
	static std::size_t getAllocationSizeForObject(int32_t length);

	int64_t castAsBigIntAndGetValue() const {
		const ValueType type = getValueType();
		if (isNull()) {
			return INT64_NULL;
		}

		switch (type) {
		case VALUE_TYPE_NULL:
			return INT64_NULL;
		case VALUE_TYPE_TINYINT:
			return static_cast<int64_t>(getTinyInt());
		case VALUE_TYPE_SMALLINT:
			return static_cast<int64_t>(getSmallInt());
		case VALUE_TYPE_INTEGER:
			return static_cast<int64_t>(getInteger());
		case VALUE_TYPE_ADDRESS:
			return getBigInt();
		case VALUE_TYPE_BIGINT:
			return getBigInt();
		case VALUE_TYPE_TIMESTAMP:
			return getTimestamp();
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT64_MAX || getDouble() < (double)INT64_MIN) {
				throw	ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			return static_cast<int64_t>(getDouble());
		default:
			throw CastException(type, VALUE_TYPE_BIGINT);
			return 0; // NOT REACHED
		}
	}

	int64_t castAsRawInt64AndGetValue() const {
		const ValueType type = getValueType();

		switch (type) {
		case VALUE_TYPE_TINYINT:
			return static_cast<int64_t>(getTinyInt());
		case VALUE_TYPE_SMALLINT:
			return static_cast<int64_t>(getSmallInt());
		case VALUE_TYPE_INTEGER:
			return static_cast<int64_t>(getInteger());
		case VALUE_TYPE_BIGINT:
			return getBigInt();
		case VALUE_TYPE_TIMESTAMP:
			return getTimestamp();
		default:
			throw CastException(type, VALUE_TYPE_BIGINT);
			return 0; // NOT REACHED
		}
	}

	double castAsDoubleAndGetValue() const {
		const ValueType type = getValueType();
		if (isNull()) {
			return DOUBLE_MIN;
		}

		switch (type) {
		case VALUE_TYPE_NULL:
			return DOUBLE_MIN;
		case VALUE_TYPE_TINYINT:
			return static_cast<double>(getTinyInt());
		case VALUE_TYPE_SMALLINT:
			return static_cast<double>(getSmallInt());
		case VALUE_TYPE_INTEGER:
			return static_cast<double>(getInteger());
		case VALUE_TYPE_ADDRESS:
			return static_cast<double>(getBigInt());
		case VALUE_TYPE_BIGINT:
			return static_cast<double>(getBigInt());
		case VALUE_TYPE_TIMESTAMP:
			return static_cast<double>(getTimestamp());
		case VALUE_TYPE_DOUBLE:
			return getDouble();
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
			return 0; // NOT REACHED
		}
	}

	TTInt castAsDecimalAndGetValue() const {
		const ValueType type = getValueType();
		if (isNull()) {
			TTInt retval;
			retval.SetMin();
			return retval;
		}

		switch (type) {
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_BIGINT:
		case VALUE_TYPE_TIMESTAMP: {
			int64_t value = castAsBigIntAndGetValue();
			TTInt retval(value);
			retval *= Value::kMaxScaleFactor;
			return retval;
		}
		case VALUE_TYPE_DECIMAL:
			return getDecimal();
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
			return 0; // NOT REACHED
		}
	}

	Value castAsBigInt() const {
		Value retval(VALUE_TYPE_BIGINT);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getBigInt() = static_cast<int64_t>(getTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.getBigInt() = static_cast<int64_t>(getSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			retval.getBigInt() = static_cast<int64_t>(getInteger()); break;
		case VALUE_TYPE_ADDRESS:
			retval.getBigInt() = getBigInt(); break;
		case VALUE_TYPE_BIGINT:
			return *this;
		case VALUE_TYPE_TIMESTAMP:
			retval.getBigInt() = getTimestamp(); break;
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT64_MAX || getDouble() < (double)INT64_MIN) {
				throw ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			retval.getBigInt() = static_cast<int64_t>(getDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_BIGINT);
		}
		return retval;
	}

	Value castAsTimestamp() const {
		Value retval(VALUE_TYPE_TIMESTAMP);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getTimestamp() = static_cast<int64_t>(getTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.getTimestamp() = static_cast<int64_t>(getSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			retval.getTimestamp() = static_cast<int64_t>(getInteger()); break;
		case VALUE_TYPE_BIGINT:
			retval.getTimestamp() = getBigInt(); break;
		case VALUE_TYPE_TIMESTAMP:
			retval.getTimestamp() = getTimestamp(); break;
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT64_MAX || getDouble() < (double)INT64_MIN) {
				throw ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			retval.getTimestamp() = static_cast<int64_t>(getDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_TIMESTAMP);
		}
		return retval;
	}

	Value castAsInteger() const {
		Value retval(VALUE_TYPE_INTEGER);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getInteger() = static_cast<int32_t>(getTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.getInteger() = static_cast<int32_t>(getSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			return *this;
		case VALUE_TYPE_BIGINT:
			if (getBigInt() > INT32_MAX || getBigInt() < INT32_MIN) {
				throw ValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
			}
			retval.getInteger() = static_cast<int32_t>(getBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (getTimestamp() > INT32_MAX || getTimestamp() < INT32_MIN) {
				throw ValueOutOfRangeException(getTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_INTEGER);
			}
			retval.getInteger() = static_cast<int32_t>(getTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT32_MAX || getDouble() < (double)INT32_MIN) {
				throw ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_INTEGER);
			}
			retval.getInteger() = static_cast<int32_t>(getDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_INTEGER);
		}
		return retval;
	}

	Value castAsSmallInt() const {
		Value retval(VALUE_TYPE_SMALLINT);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getSmallInt() = static_cast<int16_t>(getTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.getSmallInt() = getSmallInt(); break;
		case VALUE_TYPE_INTEGER:
			if (getInteger() > INT16_MAX || getInteger() < INT16_MIN) {
				throw ValueOutOfRangeException((int64_t)getInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_SMALLINT);
			}
			retval.getSmallInt() = static_cast<int16_t>(getInteger()); break;
		case VALUE_TYPE_BIGINT:
			if (getBigInt() > INT16_MAX || getBigInt() < INT16_MIN) {
				throw ValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
			}
			retval.getSmallInt() = static_cast<int16_t>(getBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (getTimestamp() > INT16_MAX || getTimestamp() < INT16_MIN) {
				throw ValueOutOfRangeException(getTimestamp(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
			}
			retval.getSmallInt() = static_cast<int16_t>(getTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT16_MAX || getDouble() < (double)INT16_MIN) {
				throw ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_SMALLINT);
			}
			retval.getSmallInt() = static_cast<int16_t>(getDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_SMALLINT);
		}
		return retval;
	}

	Value castAsTinyInt() const {
		Value retval(VALUE_TYPE_TINYINT);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getTinyInt() = getTinyInt(); break;
		case VALUE_TYPE_SMALLINT:
			if (getSmallInt() > INT8_MAX || getSmallInt() < INT8_MIN) {
				throw ValueOutOfRangeException((int64_t)getSmallInt(), VALUE_TYPE_SMALLINT, VALUE_TYPE_TINYINT);
			}
			retval.getTinyInt() = static_cast<int8_t>(getSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			if (getInteger() > INT8_MAX || getInteger() < INT8_MIN) {
				throw ValueOutOfRangeException((int64_t)getInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_TINYINT);
			}
			retval.getTinyInt() = static_cast<int8_t>(getInteger()); break;
		case VALUE_TYPE_BIGINT:
			if (getBigInt() > INT8_MAX || getBigInt() < INT8_MIN) {
				throw ValueOutOfRangeException(getBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_TINYINT);
			}
			retval.getTinyInt() = static_cast<int8_t>(getBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (getTimestamp() > INT8_MAX || getTimestamp() < INT8_MIN) {
				throw ValueOutOfRangeException(getTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_TINYINT);
			}
			retval.getTinyInt() = static_cast<int8_t>(getTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (getDouble() > (double)INT8_MAX || getDouble() < (double)INT8_MIN) {
				throw ValueOutOfRangeException(getDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_TINYINT);
			}
			retval.getTinyInt() = static_cast<int8_t>(getDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_TINYINT);
		}
		return retval;
	}

	Value castAsDouble() const {
		Value retval(VALUE_TYPE_DOUBLE);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.getDouble() = static_cast<double>(getTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.getDouble() = static_cast<double>(getSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			retval.getDouble() = static_cast<double>(getInteger()); break;
		case VALUE_TYPE_BIGINT:
			retval.getDouble() = static_cast<double>(getBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			retval.getDouble() = static_cast<double>(getTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			retval.getDouble() = getDouble(); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
		}
		return retval;
	}

	Value castAsString() const {
		Value retval(VALUE_TYPE_VARCHAR);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		// note: we allow binary conversion to strings to support
		// byte[] as string parameters...
		// In the future, it would be nice to check this is a decent string here...
		switch (type) {
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
			memcpy(retval.m_data, m_data, sizeof(m_data));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_VARCHAR);
		}
		return retval;
	}

	Value castAsBinary() const {
		Value retval(VALUE_TYPE_VARBINARY);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_VARBINARY:
			memcpy(retval.m_data, m_data, sizeof(m_data));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_VARBINARY);
		}
		return retval;
	}

	Value castAsDecimal() const {
		Value retval(VALUE_TYPE_DECIMAL);
		const ValueType type = getValueType();
		if (isNull()) {
			retval.setNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_BIGINT:
		{
			int64_t rhsint = castAsBigIntAndGetValue();
			TTInt retval(rhsint);
			retval *= Value::kMaxScaleFactor;
			return getDecimalValue(retval);
		}
		case VALUE_TYPE_DECIMAL:
			::memcpy(retval.m_data, m_data, sizeof(TTInt));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_DECIMAL);
		}
		return retval;
	}

	/**
	 * Copy the arbitrary size object that this value points to as an
	 * inline object in the provided storage area
	 */
	void inlineCopyObject(void *storage, int32_t maxLength) const {
		if (isNull()) {
			/*
			 * The 7th bit of the length preceding value
			 * is used to indicate that the object is null.
			 */
			*reinterpret_cast<char*>(storage) = OBJECT_NULL_BIT;
		}
		else {
			const int32_t objectLength = getObjectLength();
			if (objectLength > maxLength) {
				char msg[1024];
				snprintf(msg, 1024, "Object exceeds specified size. Size is %d and max is %d", objectLength, maxLength);
				throw Exception(msg);
			}
			if (m_sourceInlined)
			{
				::memcpy( storage, *reinterpret_cast<char *const *>(m_data), getObjectLengthLength() + objectLength);
			}
			else
			{
				const StringRef* sref =
						*reinterpret_cast<StringRef* const*>(m_data);
				::memcpy(storage, sref->get(),
						getObjectLengthLength() + objectLength);
			}
		}

	}

	int compareAnyIntegerValue (const Value rhs) const {
		int64_t lhsValue, rhsValue;

		// get the right hand side as a bigint
		if (rhs.getValueType() != VALUE_TYPE_BIGINT) rhsValue = rhs.castAsBigIntAndGetValue();
		else rhsValue = rhs.getBigInt();

		// convert the left hand side
		switch(getValueType()) {
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_TIMESTAMP:
			lhsValue = castAsBigIntAndGetValue(); break;
		case VALUE_TYPE_BIGINT:
			lhsValue = getBigInt(); break;
		default: {
			throw TypeMismatchException("non comparable types lhs '%d' rhs '%d'", getValueType(), rhs.getValueType());
		}
		}

		// do the comparison
		if (lhsValue == rhsValue) {
			return VALUE_COMPARE_EQUAL;
		} else if (lhsValue > rhsValue) {
			return VALUE_COMPARE_GREATERTHAN;
		} else {
			return VALUE_COMPARE_LESSTHAN;
		}
	}

	int compareDoubleValue (const Value rhs) const {
		switch (rhs.getValueType()) {
		case VALUE_TYPE_DOUBLE: {
			const double lhsValue = getDouble();
			const double rhsValue = rhs.getDouble();
			if (lhsValue == rhsValue) {
				return VALUE_COMPARE_EQUAL;
			} else if (lhsValue > rhsValue) {
				return VALUE_COMPARE_GREATERTHAN;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		}
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_BIGINT:
		case VALUE_TYPE_TIMESTAMP: {
			const double lhsValue = getDouble();
			const double rhsValue = rhs.castAsDouble().getDouble();
			if (lhsValue == rhsValue) {
				return VALUE_COMPARE_EQUAL;
			} else if (lhsValue > rhsValue) {
				return VALUE_COMPARE_GREATERTHAN;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		}
		case VALUE_TYPE_DECIMAL:
		{
			double val = rhs.castAsDoubleAndGetValue();
			if (rhs.isNegative()) {
				val *= -1;
			}
			return ((getDouble() > val) - (getDouble() < val));
		}
		default:
		{
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be cast for comparison to type %s",
					ValueToString(rhs.getValueType()).c_str(),
					ValueToString(getValueType()).c_str());
			throw Exception(message);
			// Not reached
			return 0;
		}
		}
	}

	int compareStringValue (const Value rhs) const {
		if ((rhs.getValueType() != VALUE_TYPE_VARCHAR) && (rhs.getValueType() != VALUE_TYPE_VARBINARY)) {
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be cast for comparison to type %s",
					ValueToString(rhs.getValueType()).c_str(),
					ValueToString(getValueType()).c_str());
			throw Exception(message);
		}
		const char* left = reinterpret_cast<const char*>(getObjectValue());
		const char* right = reinterpret_cast<const char*>(rhs.getObjectValue());
		if (isNull()) {
			if (rhs.isNull()) {
				return VALUE_COMPARE_EQUAL;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		} else if (rhs.isNull()) {
			return VALUE_COMPARE_GREATERTHAN;
		}
		const int32_t leftLength = getObjectLength();
		const int32_t rightLength = rhs.getObjectLength();
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

	int compareBinaryValue (const Value rhs) const {
		if (rhs.getValueType() != VALUE_TYPE_VARBINARY) {
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be cast for comparison to type %s",
					ValueToString(rhs.getValueType()).c_str(),
					ValueToString(getValueType()).c_str());
			throw Exception(message);
		}
		const char* left = reinterpret_cast<const char*>(getObjectValue());
		const char* right = reinterpret_cast<const char*>(rhs.getObjectValue());
		if (isNull()) {
			if (rhs.isNull()) {
				return VALUE_COMPARE_EQUAL;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		} else if (rhs.isNull()) {
			return VALUE_COMPARE_GREATERTHAN;
		}
		const int32_t leftLength = getObjectLength();
		const int32_t rightLength = rhs.getObjectLength();
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

	int compareDecimalValue(const Value rhs) const {
		switch (rhs.getValueType()) {
		// create the equivalent decimal value
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_BIGINT:
		{
			const TTInt lhsValue = getDecimal();
			const TTInt rhsValue = rhs.castAsDecimalAndGetValue();

			if (lhsValue == rhsValue) {
				return VALUE_COMPARE_EQUAL;
			} else if (lhsValue > rhsValue) {
				return VALUE_COMPARE_GREATERTHAN;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		}
		case VALUE_TYPE_DECIMAL:
		{
			const TTInt lhsValue = getDecimal();
			const TTInt rhsValue = rhs.getDecimal();

			if (lhsValue == rhsValue) {
				return VALUE_COMPARE_EQUAL;
			} else if (lhsValue > rhsValue) {
				return VALUE_COMPARE_GREATERTHAN;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		}
		case VALUE_TYPE_DOUBLE:
		{
			const double lhsValue = castAsDoubleAndGetValue();
			const double rhsValue = rhs.getDouble();

			if (lhsValue == rhsValue) {
				return VALUE_COMPARE_EQUAL;
			} else if (lhsValue > rhsValue) {
				return VALUE_COMPARE_GREATERTHAN;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		}
		default:
		{
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be cast for comparison to type %s",
					ValueToString(rhs.getValueType()).c_str(),
					ValueToString(getValueType()).c_str());
			throw	TypeMismatchException(message, getValueType(), rhs.getValueType());
			// Not reached
			return 0;
		}
		}
	}

	Value opAddBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return getBigIntValue(INT64_NULL);
		//Scary overflow check
		if ( ((lhs^rhs)
				| (((lhs^(~(lhs^rhs)
						& (1L << (sizeof(int64_t)*CHAR_BIT-1))))+rhs)^rhs)) >= 0) {
			char message[4096];
			snprintf(message, 4096, "Adding %jd and %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
			throw	NumericValueOutOfRangeException(message);
		}
		return getBigIntValue(lhs + rhs);
	}

	Value opSubtractBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return getBigIntValue(INT64_NULL);
		//Scary overflow check
		if ( ((lhs^rhs)
				& (((lhs ^ ((lhs^rhs)
						& (1L << (sizeof(int64_t)*CHAR_BIT-1))))-rhs)^rhs)) < 0) {
			char message[4096];
			snprintf(message, 4096, "Subtracting %jd from %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
			throw	NumericValueOutOfRangeException(message);
		}
		return getBigIntValue(lhs - rhs);
	}

	Value opMultiplyBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return getBigIntValue(INT64_NULL);
		bool overflow = false;
		//Scary overflow check
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
			throw	NumericValueOutOfRangeException(message);
		}

		return getBigIntValue(result);
	}

	Value opDivideBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return getBigIntValue(INT64_NULL);

		if (rhs == 0) {
			char message[4096];
			snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
			throw	DivideByZeroException(message);
		}

		/**
		 * Because the smallest int64 value is used to represent null (and this is checked for an handled above)
		 * it isn't necessary to check for any kind of overflow since none is possible.
		 */
		return getBigIntValue(int64_t(lhs / rhs));
	}

	Value opAddDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return getDoubleValue(DOUBLE_MIN);

		const double result = lhs + rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to add %f with %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return getDoubleValue(result);
	}

	Value opSubtractDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return getDoubleValue(DOUBLE_MIN);

		const double result = lhs - rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to subtract %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return getDoubleValue(result);
	}

	Value opMultiplyDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return getDoubleValue(DOUBLE_MIN);

		const double result = lhs * rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to multiply %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return getDoubleValue(result);
	}

	Value opDivideDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return getDoubleValue(DOUBLE_MIN);


		const double result = lhs / rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to divide %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return getDoubleValue(result);
	}

	Value opAddDecimals(const Value lhs, const Value rhs) const {
		if ((lhs.getValueType() != VALUE_TYPE_DECIMAL) ||
				(rhs.getValueType() != VALUE_TYPE_DECIMAL))
		{
			throw Exception("Non-decimal Value in decimal adder.");
		}

		if (lhs.isNull() || rhs.isNull()) {
			TTInt retval;
			retval.SetMin();
			return getDecimalValue(retval);
		}

		TTInt retval(lhs.getDecimal());
		if (retval.Add(rhs.getDecimal()) || retval > Value::s_maxDecimal || retval < s_minDecimal) {
			char message[4096];
			snprintf(message, 4096, "Attempted to add %s with %s causing overflow/underflow",
					lhs.createStringFromDecimal().c_str(), rhs.createStringFromDecimal().c_str());
			throw	NumericValueOutOfRangeException(message);
		}

		return getDecimalValue(retval);
	}

	Value opSubtractDecimals(const Value lhs, const Value rhs) const {
		if ((lhs.getValueType() != VALUE_TYPE_DECIMAL) ||
				(rhs.getValueType() != VALUE_TYPE_DECIMAL))
		{
			throw Exception("Non-decimal Value in decimal subtract.");
		}

		if (lhs.isNull() || rhs.isNull()) {
			TTInt retval;
			retval.SetMin();
			return getDecimalValue(retval);
		}

		TTInt retval(lhs.getDecimal());
		if (retval.Sub(rhs.getDecimal()) || retval > Value::s_maxDecimal || retval < Value::s_minDecimal) {
			char message[4096];
			snprintf(message, 4096, "Attempted to subtract %s from %s causing overflow/underflow",
					rhs.createStringFromDecimal().c_str(), lhs.createStringFromDecimal().c_str());
			throw	NumericValueOutOfRangeException(message);
		}

		return getDecimalValue(retval);
	}

	static Value getTinyIntValue(int8_t value) {
		Value retval(VALUE_TYPE_TINYINT);
		retval.getTinyInt() = value;
		return retval;
	}

	static Value getSmallIntValue(int16_t value) {
		Value retval(VALUE_TYPE_SMALLINT);
		retval.getSmallInt() = value;
		return retval;
	}

	static Value getIntegerValue(int32_t value) {
		Value retval(VALUE_TYPE_INTEGER);
		retval.getInteger() = value;
		return retval;
	}

	static Value getBigIntValue(int64_t value) {
		Value retval(VALUE_TYPE_BIGINT);
		retval.getBigInt() = value;
		return retval;
	}

	static Value getTimestampValue(int64_t value) {
		Value retval(VALUE_TYPE_TIMESTAMP);
		retval.getTimestamp() = value;
		return retval;
	}

	static Value getDoubleValue(double value) {
		Value retval(VALUE_TYPE_DOUBLE);
		retval.getDouble() = value;
		return retval;
	}

	static Value getDecimalValueFromString(const std::string &value) {
		Value retval(VALUE_TYPE_DECIMAL);
		retval.createDecimalFromString(value);
		return retval;
	}

	static Value getStringValue(std::string value) {
		Value retval(VALUE_TYPE_VARCHAR);
		const int32_t length = static_cast<int32_t>(value.length());
		const int8_t lengthLength = getAppropriateObjectLengthLength(length);
		const int32_t minLength = length + lengthLength;
		StringRef* sref = StringRef::create(minLength);
		char* storage = sref->get();
		setObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, value.c_str(), length);
		retval.setObjectValue(sref);
		retval.setObjectLength(length);
		retval.setObjectLengthLength(lengthLength);
		return retval;
	}

	// assumes binary value in hex
	static Value getBinaryValue(const std::string value) {
		Value retval(VALUE_TYPE_VARBINARY);
		const int32_t length = static_cast<int32_t>(value.length() / 2);

		boost::shared_array<unsigned char> buf(new unsigned char[length]);
		hexDecodeToBinary(buf.get(), value.c_str());

		const int8_t lengthLength = getAppropriateObjectLengthLength(length);
		const int32_t minLength = length + lengthLength;
		StringRef* sref = StringRef::create(minLength);
		char* storage = sref->get();
		setObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, buf.get(), length);
		retval.setObjectValue(sref);
		retval.setObjectLength(length);
		retval.setObjectLengthLength(lengthLength);
		return retval;
	}

	static Value getBinaryValue(const unsigned char *value, const int32_t length) {
		Value retval(VALUE_TYPE_VARBINARY);
		const int8_t lengthLength = getAppropriateObjectLengthLength(length);
		const int32_t minLength = length + lengthLength;
		StringRef* sref = StringRef::create(minLength);
		char* storage = sref->get();
		setObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, value, length);
		retval.setObjectValue(sref);
		retval.setObjectLength(length);
		retval.setObjectLengthLength(lengthLength);
		return retval;
	}

	static Value getNullStringValue() {
		Value retval(VALUE_TYPE_VARCHAR);
		*reinterpret_cast<char**>(retval.m_data) = NULL;
		return retval;
	}

	static Value getNullBinaryValue() {
		Value retval(VALUE_TYPE_VARBINARY);
		*reinterpret_cast<char**>(retval.m_data) = NULL;
		return retval;
	}

	static Value getNullValue() {
		Value retval(VALUE_TYPE_NULL);
		return retval;
	}

	static Value getDecimalValue(TTInt value) {
		Value retval(VALUE_TYPE_DECIMAL);
		retval.getDecimal() = value;
		return retval;
	}

	static Value getAddressValue(void *address) {
		Value retval(VALUE_TYPE_ADDRESS);
		*reinterpret_cast<void**>(retval.m_data) = address;
		return retval;
	}
};

/**
 * Public constructor that initializes to an Value that is unusable
 * with other Values.  Useful for declaring storage for an Value.
 */
inline Value::Value() {
	::memset( m_data, 0, 16);
	setValueType(VALUE_TYPE_INVALID);
}

/**
 * Retrieve a boolean Value that is true
 */
inline Value Value::getTrue() {
	Value retval(VALUE_TYPE_BOOLEAN);
	retval.getBoolean() = true;
	return retval;
}

/**
 * Retrieve a boolean Value that is false
 */
inline Value Value::getFalse() {
	Value retval(VALUE_TYPE_BOOLEAN);
	retval.getBoolean() = false;
	return retval;
}

/**
 * Return a new Value that is the opposite of this one. Only works on
 * booleans
 */
inline Value Value::op_negate() const {
	assert(getValueType() == VALUE_TYPE_BOOLEAN);
	Value retval(VALUE_TYPE_BOOLEAN);
	retval.getBoolean() = !getBoolean();
	return retval;
}

/**
 * Returns C++ true if this Value is a boolean and is true
 */
inline bool Value::isTrue() const {
	assert(getValueType() == VALUE_TYPE_BOOLEAN);
	return getBoolean();
}

/**
 * Returns C++ false if this Value is a boolean and is true
 */
inline bool Value::isFalse() const {
	assert(getValueType() == VALUE_TYPE_BOOLEAN);
	return !getBoolean();
}

inline bool Value::isNegative() const {
	const ValueType type = getValueType();
	switch (type) {
	case VALUE_TYPE_TINYINT:
		return getTinyInt() < 0;
	case VALUE_TYPE_SMALLINT:
		return getSmallInt() < 0;
	case VALUE_TYPE_INTEGER:
		return getInteger() < 0;
	case VALUE_TYPE_BIGINT:
		return getBigInt() < 0;
	case VALUE_TYPE_TIMESTAMP:
		return getTimestamp() < 0;
	case VALUE_TYPE_DOUBLE:
		return getDouble() < 0;
	case VALUE_TYPE_DECIMAL:
		return getDecimal().IsSign();
	default: {
		throw UnknownTypeException(type, "Invalid value type for checking negativity");
	}
	}
}

/**
 * Logical and operation for Values
 */
inline Value Value::op_and(const Value rhs) const {
	if (getBoolean() && rhs.getBoolean()) {
		return getTrue();
	}
	return getFalse();
}

/*
 * Logical or operation for Values
 */
inline Value Value::op_or(const Value rhs) const {
	if(getBoolean() || rhs.getBoolean()) {
		return getTrue();
	}
	return getFalse();
}

/**
 * Objects may have storage allocated for them. Calling free causes the Value to return the storage allocated for
 * the object to the heap
 */
inline void Value::free() const {
	switch (getValueType())
	{
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		assert(!m_sourceInlined);
		StringRef* sref = *reinterpret_cast<StringRef* const*>(m_data);
		if (sref != NULL)
		{
			StringRef::destroy(sref);
		}
	}
	break;
	default:
		return;
	}
}

/**
 * Get the amount of storage necessary to store a value of the specified type
 * in a tuple
 */
inline uint16_t Value::getTupleStorageSize(const ValueType type) {
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
	default:
		char message[128];
		snprintf(message, 128, "Value::getTupleStorageSize() unrecognized type"
				" '%d'", type);
		throw Exception(message);
	}
}

/**
 * Compare any two Values. Comparison is not guaranteed to
 * succeed if the values are incompatible.  Avoid use of
 * comparison in favor of op_*.
 */
inline int Value::compare(const Value rhs) const {
	switch (getValueType()) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
		if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
			return castAsDouble().compareDoubleValue(rhs);
		} else if (rhs.getValueType() == VALUE_TYPE_DECIMAL) {
			return -1 * rhs.compareDecimalValue(*this);
		} else {
			return compareAnyIntegerValue(rhs);
		}
	case VALUE_TYPE_TIMESTAMP:
		if (rhs.getValueType() == VALUE_TYPE_DOUBLE) {
			return castAsDouble().compareDoubleValue(rhs);
		} else {
			return compareAnyIntegerValue(rhs);
		}
	case VALUE_TYPE_DOUBLE:
		return compareDoubleValue(rhs);
	case VALUE_TYPE_VARCHAR:
		return compareStringValue(rhs);
	case VALUE_TYPE_DECIMAL:
		return compareDecimalValue(rhs);
	default: {
		throw IncompatibleTypeException(rhs.getValueType(), "non comparable type '%d'");
	}
	}
}

/**
 * Set this Value to null.
 */
inline void Value::setNull() {
	switch (getValueType()) {
	case VALUE_TYPE_NULL:
	case VALUE_TYPE_INVALID:
		return;
	case VALUE_TYPE_TINYINT:
		getTinyInt() = INT8_NULL;
		break;
	case VALUE_TYPE_SMALLINT:
		getSmallInt() = INT16_NULL;
		break;
	case VALUE_TYPE_INTEGER:
		getInteger() = INT32_NULL;
		break;
	case VALUE_TYPE_TIMESTAMP:
		getTimestamp() = INT64_NULL;
		break;
	case VALUE_TYPE_BIGINT:
		getBigInt() = INT64_NULL;
		break;
	case VALUE_TYPE_DOUBLE:
		getDouble() = DOUBLE_MIN;
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		*reinterpret_cast<void**>(m_data) = NULL;
		break;
	case VALUE_TYPE_DECIMAL:
		getDecimal().SetMin();
		break;
	default: {
		throw IncompatibleTypeException(getValueType(), "Value::setNull() called with ValueType '%d'");
	}
	}
}

/**
 * Deserialize a scalar of the specified type from the tuple
 * storage area provided. If this is an Object type then the third
 * argument indicates whether the object is stored in the tuple
 * inline TODO: Could the isInlined argument be removed by have
 * the caller dereference the pointer?
 */
inline const Value Value::deserializeFromTupleStorage(const void *storage,
		const ValueType type,
		const bool isInlined)
{
	Value retval(type);
	switch (type)
	{
	case VALUE_TYPE_TIMESTAMP:
		retval.getTimestamp() = *reinterpret_cast<const int64_t*>(storage);
		break;
	case VALUE_TYPE_TINYINT:
		retval.getTinyInt() = *reinterpret_cast<const int8_t*>(storage);
		break;
	case VALUE_TYPE_SMALLINT:
		retval.getSmallInt() = *reinterpret_cast<const int16_t*>(storage);
		break;
	case VALUE_TYPE_INTEGER:
		retval.getInteger() = *reinterpret_cast<const int32_t*>(storage);
		break;
	case VALUE_TYPE_BIGINT:
		retval.getBigInt() = *reinterpret_cast<const int64_t*>(storage);
		break;
	case VALUE_TYPE_DOUBLE:
		retval.getDouble() = *reinterpret_cast<const double*>(storage);
		break;
	case VALUE_TYPE_DECIMAL:
		::memcpy( retval.m_data, storage, Value::getTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		//Potentially non-inlined type requires special handling
		char* data = NULL;
		if (isInlined) {
			//If it is inlined the storage area contains the actual data so copy a reference
			//to the storage area
			*reinterpret_cast<void**>(retval.m_data) = const_cast<void*>(storage);
			data = *reinterpret_cast<char**>(retval.m_data);
			retval.setSourceInlined(true);
		} else {
			//If it isn't inlined the storage area contains a pointer to the
			// StringRef object containing the string's memory
			memcpy( retval.m_data, storage, sizeof(void*));
			StringRef* sref = *reinterpret_cast<StringRef**>(retval.m_data);
			// If the StringRef pointer is null, that's because this
			// was a null value; leave the data pointer as NULL so
			// that getObjectLengthFromLocation will figure this out
			// correctly, otherwise get the right char* from the StringRef
			if (sref != NULL)
			{
				data = sref->get();
			}
		}
		const int32_t length = getObjectLengthFromLocation(data);
		//std::cout << "Value::deserializeFromTupleStorage: length: " << length << std::endl;
		retval.setObjectLength(length);
		retval.setObjectLengthLength(getAppropriateObjectLengthLength(length));
		break;
	}
	default:
		throw IncompatibleTypeException(type, "Value::getLength() unrecognized type '%d'");
	}
	return retval;
}

/**
 * Serialize the scalar this Value represents to the provided
 * storage area. If the scalar is an Object type that is not
 * inlined then the provided data pool or the heap will be used to
 * allocated storage for a copy of the object.
 */
inline void Value::serializeToTupleStorageAllocateForObjects(void *storage, const bool isInlined,
		const int32_t maxLength, Pool *dataPool) const
{
	const ValueType type = getValueType();
	int32_t length = 0;

	switch (type) {
	case VALUE_TYPE_TIMESTAMP:
		*reinterpret_cast<int64_t*>(storage) = getTimestamp();
		break;
	case VALUE_TYPE_TINYINT:
		*reinterpret_cast<int8_t*>(storage) = getTinyInt();
		break;
	case VALUE_TYPE_SMALLINT:
		*reinterpret_cast<int16_t*>(storage) = getSmallInt();
		break;
	case VALUE_TYPE_INTEGER:
		*reinterpret_cast<int32_t*>(storage) = getInteger();
		break;
	case VALUE_TYPE_BIGINT:
		*reinterpret_cast<int64_t*>(storage) = getBigInt();
		break;
	case VALUE_TYPE_DOUBLE:
		*reinterpret_cast<double*>(storage) = getDouble();
		break;
	case VALUE_TYPE_DECIMAL:
		::memcpy( storage, m_data, Value::getTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		//Potentially non-inlined type requires special handling
		if (isInlined) {
			inlineCopyObject(storage, maxLength);
		}
		else {
			if (isNull()) {
				*reinterpret_cast<void**>(storage) = NULL;
			}
			else {
				length = getObjectLength();
				const int8_t lengthLength = getObjectLengthLength();
				const int32_t minlength = lengthLength + length;
				if (length > maxLength) {
					char msg[1024];
					snprintf(msg, 1024, "Object exceeds specified size. Size is %d"
							" and max is %d", length, maxLength);
					throw ObjectSizeException(msg);
				}
				StringRef* sref = StringRef::create(minlength, dataPool);
				char *copy = sref->get();
				setObjectLengthToLocation(length, copy);
				::memcpy(copy + lengthLength, getObjectValue(), length);
				*reinterpret_cast<StringRef**>(storage) = sref;
			}
		}
		break;
	default: {
		throw UnknownTypeException(type, "Value::serializeToTupleStorageAllocateForObjects() unrecognized type '%d'");
	}
	}
}

/**
 * Serialize the scalar this Value represents to the storage area
 * provided. If the scalar is an Object type then the object will
 * be copy if it can be inlined into the tuple. Otherwise a
 * pointer to the object will be copied into the storage area. No
 * allocations are performed.
 */
inline void Value::serializeToTupleStorage(void *storage, const bool isInlined, const int32_t maxLength) const
{
	const ValueType type = getValueType();
	switch (type) {
	case VALUE_TYPE_TIMESTAMP:
		*reinterpret_cast<int64_t*>(storage) = getTimestamp();
		break;
	case VALUE_TYPE_TINYINT:
		*reinterpret_cast<int8_t*>(storage) = getTinyInt();
		break;
	case VALUE_TYPE_SMALLINT:
		*reinterpret_cast<int16_t*>(storage) = getSmallInt();
		break;
	case VALUE_TYPE_INTEGER:
		*reinterpret_cast<int32_t*>(storage) = getInteger();
		break;
	case VALUE_TYPE_BIGINT:
		*reinterpret_cast<int64_t*>(storage) = getBigInt();
		break;
	case VALUE_TYPE_DOUBLE:
		*reinterpret_cast<double*>(storage) = getDouble();
		break;
	case VALUE_TYPE_DECIMAL:
		::memcpy( storage, m_data, Value::getTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		//Potentially non-inlined type requires special handling
		if (isInlined) {
			inlineCopyObject(storage, maxLength);
		}
		else {
			if (isNull() || getObjectLength() <= maxLength) {
				if (m_sourceInlined && !isInlined)
				{
					throw Exception("Cannot serialize an inlined string to non-inlined tuple storage in serializeToTupleStorage()");
				}
				// copy the StringRef pointers
				*reinterpret_cast<StringRef**>(storage) =
						*reinterpret_cast<StringRef* const*>(m_data);
			}
			else {
				const int32_t length = getObjectLength();
				char msg[1024];
				snprintf(msg, 1024, "Object exceeds specified size. Size is %d and max is %d", length, maxLength);
				throw ObjectSizeException(msg);
			}
		}
		break;
	default:
		char message[128];
		snprintf(message, 128, "Value::serializeToTupleStorage() unrecognized type '%d'", type);
		throw ObjectSizeException(message);
	}
}


/**
 * Deserialize a scalar value of the specified type from the
 * SerializeInput directly into the tuple storage area
 * provided. This function will perform memory allocations for
 * Object types as necessary using the provided data pool or the
 * heap. This is used to deserialize tables.
 */
inline int64_t Value::deserializeFrom(SerializeInput &input, const ValueType type,
		char *storage, bool isInlined, const int32_t maxLength, Pool *dataPool) {
	switch (type) {
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		*reinterpret_cast<int64_t*>(storage) = input.readLong();
		return sizeof(int64_t);
	case VALUE_TYPE_TINYINT:
		*reinterpret_cast<int8_t*>(storage) = input.readByte();
		return sizeof(int8_t);
	case VALUE_TYPE_SMALLINT:
		*reinterpret_cast<int16_t*>(storage) = input.readShort();
		return sizeof(int16_t);
	case VALUE_TYPE_INTEGER:
		*reinterpret_cast<int32_t*>(storage) = input.readInt();
		return sizeof(int32_t);
	case VALUE_TYPE_DOUBLE:
		*reinterpret_cast<double* >(storage) = input.readDouble();
		return sizeof(double);
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		int64_t bytesRead = 0;
		const int32_t length = input.readInt();
		bytesRead+= sizeof(int32_t);
		if (length > maxLength) {
			char msg[1024];
			snprintf(msg, 1024, "String exceeds specified size. Size is %d and max is %d", length, maxLength);
			throw ObjectSizeException(msg);
		}

		const int8_t lengthLength = getAppropriateObjectLengthLength(length);
		// the NULL SQL string is a NULL C pointer
		if (isInlined) {
			setObjectLengthToLocation(length, storage);
			if (length == OBJECTLENGTH_NULL) {
				return 0;
			}
			const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
			::memcpy( storage + lengthLength, data, length);
		} else {
			if (length == OBJECTLENGTH_NULL) {
				*reinterpret_cast<void**>(storage) = NULL;
				return 0;
			}
			const char *data = reinterpret_cast<const char*>(input.getRawPointer(length));
			const int32_t minlength = lengthLength + length;
			StringRef* sref = StringRef::create(minlength, dataPool);
			char* copy = sref->get();
			setObjectLengthToLocation( length, copy);
			::memcpy(copy + lengthLength, data, length);
			*reinterpret_cast<StringRef**>(storage) = sref;
		}
		bytesRead+=length;
		return bytesRead;
	}
	case VALUE_TYPE_DECIMAL: {
		int64_t *longStorage = reinterpret_cast<int64_t*>(storage);
		//Reverse order for Java BigDecimal BigEndian
		longStorage[1] = input.readLong();
		longStorage[0] = input.readLong();
		return 2*sizeof(long);
	}
	default:
		char message[128];
		snprintf(message, 128, "Value::deserializeFrom() unrecognized type '%d'",
				type);
		throw UnknownTypeException(type, message);
	}
}

/**
 * Deserialize a scalar value of the specified type from the
 * provided SerializeInput and perform allocations as necessary.
 * This is used to deserialize parameter sets.
 */
inline const Value Value::deserializeFromAllocateForStorage(SerializeInput &input, Pool *dataPool) {
	const ValueType type = static_cast<ValueType>(input.readByte());
	Value retval(type);
	switch (type) {
	case VALUE_TYPE_BIGINT:
		retval.getBigInt() = input.readLong();
		break;
	case VALUE_TYPE_TIMESTAMP:
		retval.getTimestamp() = input.readLong();
		break;
	case VALUE_TYPE_TINYINT:
		retval.getTinyInt() = input.readByte();
		break;
	case VALUE_TYPE_SMALLINT:
		retval.getSmallInt() = input.readShort();
		break;
	case VALUE_TYPE_INTEGER:
		retval.getInteger() = input.readInt();
		break;
	case VALUE_TYPE_DOUBLE:
		retval.getDouble() = input.readDouble();
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		const int32_t length = input.readInt();
		const int8_t lengthLength = getAppropriateObjectLengthLength(length);
		// the NULL SQL string is a NULL C pointer
		if (length == OBJECTLENGTH_NULL) {
			retval.setNull();
			break;
		}
		const void *str = input.getRawPointer(length);
		const int32_t minlength = lengthLength + length;
		StringRef* sref = StringRef::create(minlength, dataPool);
		char* copy = sref->get();
		retval.setObjectLengthToLocation( length, copy);
		::memcpy(copy + lengthLength, str, length);
		retval.setObjectValue(sref);
		retval.setObjectLength(length);
		retval.setObjectLengthLength(lengthLength);
		break;
	}
	case VALUE_TYPE_DECIMAL: {
		retval.getDecimal().table[1] = input.readLong();
		retval.getDecimal().table[0] = input.readLong();
		break;

	}
	case VALUE_TYPE_NULL: {
		retval.setNull();
		break;
	}
	default:
		throw UnknownTypeException(type, "Value::deserializeFromAllocateForStorage() unrecognized type '%d'");
	}
	return retval;
}

/**
 * Serialize this Value to the provided SerializeOutput
 */
inline void Value::serializeTo(SerializeOutput &output) const {
	const ValueType type = getValueType();
	switch (type) {
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		if (isNull()) {
			output.writeInt(OBJECTLENGTH_NULL);
			break;
		}
		const int32_t length = getObjectLength();
		if (length < OBJECTLENGTH_NULL) {
			throw Exception("Attempted to serialize an Value with a negative length");
		}
		output.writeInt(static_cast<int32_t>(length));
		if (length != OBJECTLENGTH_NULL) {
			// Not a null string: write it out
			const char * str = reinterpret_cast<const char*>(getObjectValue());
			if (str == NULL) {}
			output.writeBytes(getObjectValue(), length);
		} else {
			assert(getObjectValue() == NULL || length == OBJECTLENGTH_NULL);
		}

		break;
	}
	case VALUE_TYPE_TINYINT: {
		output.writeByte(getTinyInt());
		break;
	}
	case VALUE_TYPE_SMALLINT: {
		output.writeShort(getSmallInt());
		break;
	}
	case VALUE_TYPE_INTEGER: {
		output.writeInt(getInteger());
		break;
	}
	case VALUE_TYPE_TIMESTAMP: {
		output.writeLong(getTimestamp());
		break;
	}
	case VALUE_TYPE_BIGINT: {
		output.writeLong(getBigInt());
		break;
	}
	case VALUE_TYPE_DOUBLE: {
		output.writeDouble(getDouble());
		break;
	}
	case VALUE_TYPE_DECIMAL: {
		output.writeLong(getDecimal().table[1]);
		output.writeLong(getDecimal().table[0]);
		break;
	}
	default:
		throw UnknownTypeException(type, "Value::serializeTo() found a column "
				"with ValueType '%d' that is not handled");
	}
}

inline void Value::serializeToExport(ExportSerializeOutput &io) const
{
	switch (getValueType()) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
	{
		int64_t val = castAsBigIntAndGetValue();
		io.writeLong(val);
		return;
	}
	case VALUE_TYPE_DOUBLE:
	{
		double value = getDouble();
		io.writeDouble(value);
		return;
	}
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		// requires (and uses) bytecount not character count
		io.writeBinaryString(getObjectValue(), getObjectLength());
		return;
	}
	case VALUE_TYPE_DECIMAL:
	{
		std::string decstr = createStringFromDecimal();
		int32_t objectLength = (int32_t)decstr.length();
		io.writeBinaryString(decstr.data(), objectLength);
		return;
	}
	case VALUE_TYPE_INVALID:
	case VALUE_TYPE_NULL:
	case VALUE_TYPE_BOOLEAN:
	case VALUE_TYPE_ADDRESS:
		char message[128];
		snprintf(message, 128, "Invalid type in serializeToExport: %d", getValueType());
		throw UnknownTypeException(getValueType(), message);
	}

	throw UnknownTypeException(getValueType(), "");
}

inline bool Value::isNull() const {
	switch (getValueType()) {
	case VALUE_TYPE_NULL:
	case VALUE_TYPE_INVALID:
		return true;
	case VALUE_TYPE_TINYINT:
		return getTinyInt() == INT8_NULL;
	case VALUE_TYPE_SMALLINT:
		return getSmallInt() == INT16_NULL;
	case VALUE_TYPE_INTEGER:
		return getInteger() == INT32_NULL;
	case VALUE_TYPE_TIMESTAMP:
	case VALUE_TYPE_BIGINT:
		return getBigInt() == INT64_NULL;
	case VALUE_TYPE_ADDRESS:
		return *reinterpret_cast<void* const*>(m_data) == NULL;
	case VALUE_TYPE_DOUBLE:
		return getDouble() <= DOUBLE_NULL;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		return *reinterpret_cast<void* const*>(m_data) == NULL ||
				*reinterpret_cast<const int32_t*>(&m_data[8]) == OBJECTLENGTH_NULL;
	case VALUE_TYPE_DECIMAL: {
		TTInt min;
		min.SetMin();
		return getDecimal() == min;
	}
	default:
		throw IncompatibleTypeException(getValueType(), "Value::isNull() called with ValueType '%d'");
	}
	return false;
}

inline Value Value::op_equals(const Value rhs) const {
	return compare(rhs) == 0 ? getTrue() : getFalse();
}

inline Value Value::op_notEquals(const Value rhs) const {
	return compare(rhs) != 0 ? getTrue() : getFalse();
}

inline Value Value::op_lessThan(const Value rhs) const {
	return compare(rhs) < 0 ? getTrue() : getFalse();
}

inline Value Value::op_lessThanOrEqual(const Value rhs) const {
	return compare(rhs) <= 0 ? getTrue() : getFalse();
}

inline Value Value::op_greaterThan(const Value rhs) const {
	return compare(rhs) > 0 ? getTrue() : getFalse();
}

inline Value Value::op_greaterThanOrEqual(const Value rhs) const {
	return compare(rhs) >= 0 ? getTrue() : getFalse();
}

inline Value Value::op_max(const Value rhs) const {
	const int value = compare(rhs);
	if (value > 0) {
		return *this;
	} else {
		return rhs;
	}
}

inline Value Value::op_min(const Value rhs) const {
	const int value = compare(rhs);
	if (value < 0) {
		return *this;
	} else {
		return rhs;
	}
}

inline Value Value::getNullValue(ValueType type) {
	Value retval(type);
	retval.setNull();
	return retval;
}

inline void Value::hashCombine(std::size_t &seed) const {
	const ValueType type = getValueType();
	switch (type) {
	case VALUE_TYPE_TINYINT:
		boost::hash_combine( seed, getTinyInt()); break;
	case VALUE_TYPE_SMALLINT:
		boost::hash_combine( seed, getSmallInt()); break;
	case VALUE_TYPE_INTEGER:
		boost::hash_combine( seed, getInteger()); break;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		boost::hash_combine( seed, getBigInt()); break;
	case VALUE_TYPE_DOUBLE:
		boost::hash_combine( seed, getDouble()); break;
	case VALUE_TYPE_VARCHAR: {
		if (getObjectValue() == NULL) {
			boost::hash_combine( seed, std::string(""));
		} else {
			const int32_t length = getObjectLength();
			boost::hash_combine( seed, std::string( reinterpret_cast<const char*>(getObjectValue()), length ));
		}
		break;
	}
	case VALUE_TYPE_VARBINARY: {
		if (getObjectValue() == NULL) {
			boost::hash_combine( seed, std::string(""));
		} else {
			const int32_t length = getObjectLength();
			char* data = reinterpret_cast<char*>(getObjectValue());
			for (int32_t i = 0; i < length; i++)
				boost::hash_combine(seed, data[i]);
		}
		break;
	}
	case VALUE_TYPE_DECIMAL:
		getDecimal().hash(seed); break;
	default:
		throw UnknownTypeException((int) type, "unknown type %d");
	}
}


inline Value Value::castAs(ValueType type) const {
	if (getValueType() == type) {
		return *this;
	}

	switch (type) {
	case VALUE_TYPE_TINYINT:
		return castAsTinyInt();
	case VALUE_TYPE_SMALLINT:
		return castAsSmallInt();
	case VALUE_TYPE_INTEGER:
		return castAsInteger();
	case VALUE_TYPE_BIGINT:
		return castAsBigInt();
	case VALUE_TYPE_TIMESTAMP:
		return castAsTimestamp();
	case VALUE_TYPE_DOUBLE:
		return castAsDouble();
	case VALUE_TYPE_VARCHAR:
		return castAsString();
	case VALUE_TYPE_VARBINARY:
		return castAsBinary();
	case VALUE_TYPE_DECIMAL:
		return castAsDecimal();
	default:
		char message[128];
		snprintf(message, 128, "Type %d not a recognized type for casting",
				(int) type);

		throw TypeMismatchException(message, getValueType(), type);
	}
}

inline void* Value::castAsAddress() const {
	const ValueType type = getValueType();
	switch (type) {
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_ADDRESS:
		return *reinterpret_cast<void* const*>(m_data);
	default:
		throw UnknownTypeException((int) type, "Type %d not a recognized type for casting as an address");
	}
}

inline Value Value::op_increment() const {
	const ValueType type = getValueType();
	Value retval(type);
	switch(type) {
	case VALUE_TYPE_TINYINT:
		if (getTinyInt() == INT8_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this TinyInt results in a value out of range");
		}
		retval.getTinyInt() = static_cast<int8_t>(getTinyInt() + 1); break;
	case VALUE_TYPE_SMALLINT:
		if (getSmallInt() == INT16_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this SmallInt results in a value out of range");
		}
		retval.getSmallInt() = static_cast<int16_t>(getSmallInt() + 1); break;
	case VALUE_TYPE_INTEGER:
		if (getInteger() == INT32_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this Integer results in a value out of range");
		}
		retval.getInteger() = getInteger() + 1; break;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		if (getBigInt() == INT64_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this BigInt/Timestamp results in a value out of range");
		}
		retval.getBigInt() = getBigInt() + 1; break;
	case VALUE_TYPE_DOUBLE:
		retval.getDouble() = getDouble() + 1; break;
	default:
		throw IncompatibleTypeException((int) type, "type %d is not incrementable");
		break;
	}
	return retval;
}

inline Value Value::op_decrement() const {
	const ValueType type = getValueType();
	Value retval(type);
	switch(type) {
	case VALUE_TYPE_TINYINT:
		if (getTinyInt() == INT8_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this TinyInt results in a value out of range");
		}
		retval.getTinyInt() = static_cast<int8_t>(getTinyInt() - 1); break;
	case VALUE_TYPE_SMALLINT:
		if (getSmallInt() == INT16_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this SmallInt results in a value out of range");
		}
		retval.getSmallInt() = static_cast<int16_t>(getSmallInt() - 1); break;
	case VALUE_TYPE_INTEGER:
		if (getInteger() == INT32_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this Integer results in a value out of range");
		}
		retval.getInteger() = getInteger() - 1; break;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		if (getBigInt() == INT64_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this BigInt/Timestamp results in a value out of range");
		}
		retval.getBigInt() = getBigInt() - 1; break;
	case VALUE_TYPE_DOUBLE:
		retval.getDouble() = getDouble() - 1; break;
	default:
		throw IncompatibleTypeException ((int) type, "type %d is not decrementable");
		break;
	}
	return retval;
}

inline bool Value::isZero() const {
	const ValueType type = getValueType();
	switch(type) {
	case VALUE_TYPE_TINYINT:
		return getTinyInt() == 0;
	case VALUE_TYPE_SMALLINT:
		return getSmallInt() == 0;
	case VALUE_TYPE_INTEGER:
		return getInteger() == 0;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		return getBigInt() == 0;
	case VALUE_TYPE_DECIMAL:
		return getDecimal().IsZero();
	default:
		throw IncompatibleTypeException ((int) type, "type %d is not a numeric type that implements isZero()");
	}
}

inline Value Value::op_subtract(const Value rhs) const {
	ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
	switch (vt) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		return opSubtractBigInts(castAsBigIntAndGetValue(),
				rhs.castAsBigIntAndGetValue());

	case VALUE_TYPE_DOUBLE:
		return opSubtractDoubles(castAsDoubleAndGetValue(),
				rhs.castAsDoubleAndGetValue());

	case VALUE_TYPE_DECIMAL:
		return opSubtractDecimals(castAsDecimal(),
				rhs.castAsDecimal());

	default:
		break;
	}
	throw TypeMismatchException("Promotion of %s and %s failed in op_subtract.",
			getValueType(),
			rhs.getValueType());
}

inline Value Value::op_add(const Value rhs) const {
	ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
	switch (vt) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		return opAddBigInts(castAsBigIntAndGetValue(),
				rhs.castAsBigIntAndGetValue());

	case VALUE_TYPE_DOUBLE:
		return opAddDoubles(castAsDoubleAndGetValue(),
				rhs.castAsDoubleAndGetValue());

	case VALUE_TYPE_DECIMAL:
		return opAddDecimals(castAsDecimal(),
				rhs.castAsDecimal());

	default:
		break;
	}
	throw TypeMismatchException("Promotion of %s and %s failed in op_add.",
			getValueType(),
			rhs.getValueType());
}

inline Value Value::op_multiply(const Value rhs) const {
	ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
	switch (vt) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		return opMultiplyBigInts(castAsBigIntAndGetValue(),
				rhs.castAsBigIntAndGetValue());

	case VALUE_TYPE_DOUBLE:
		return opMultiplyDoubles(castAsDoubleAndGetValue(),
				rhs.castAsDoubleAndGetValue());

	case VALUE_TYPE_DECIMAL:
		return opMultiplyDecimals(*this, rhs);

	default:
		break;
	}
	throw TypeMismatchException("Promotion of %s and %s failed in op_multiply.",
			getValueType(),
			rhs.getValueType());
}

inline Value Value::op_divide(const Value rhs) const {
	ValueType vt = promoteForOp(getValueType(), rhs.getValueType());
	switch (vt) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		return opDivideBigInts(castAsBigIntAndGetValue(),
				rhs.castAsBigIntAndGetValue());

	case VALUE_TYPE_DOUBLE:
		return opDivideDoubles(castAsDoubleAndGetValue(),
				rhs.castAsDoubleAndGetValue());

	case VALUE_TYPE_DECIMAL:
		return opDivideDecimals(castAsDecimal(),
				rhs.castAsDecimal());

	default:
		break;
	}

	throw TypeMismatchException("Promotion of %s and %s failed in op_divide.",
			getValueType(),
			rhs.getValueType());
}



} // End nstore namespace


