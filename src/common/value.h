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

#include "ttmath/ttmathint.h"
#include "boost/scoped_ptr.hpp"

#include "common/exception.h"
#include "common/pool.h"
#include "common/serializer.h"
#include "common/types.h"
#include "common/varlen.h"

namespace nstore {

//===--------------------------------------------------------------------===//
// Type system
//===--------------------------------------------------------------------===//

#define CHECK_FPE( x ) ( std::isinf(x) || std::isnan(x) )

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

/// The int used for storage and return values
typedef ttmath::Int<2> TTInt;

/// Long integer with space for multiplication and division without carry/overflow
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
	/// Create a default Value
	Value();

	/// Release memory associated to object type Values
	void FreeUninlinedData() const;


	//===--------------------------------------------------------------------===//
	// Core Functions
	//===--------------------------------------------------------------------===//

	/// Create an Value promoted/demoted to type
	Value CastAs(ValueType type) const;

	/// Reveal the contained pointer for type values
	void* CastAsAddress() const;

	//===--------------------------------------------------------------------===//
	// NULLs and BOOLs
	//===--------------------------------------------------------------------===//

	/// Set value to the correct SQL NULL representation.
	void SetNull();

	/// Create a boolean true Value
	static Value GetTrue();

	/// Create a boolean false Value
	static Value GetFalse();

	/// Create an Value with the null representation for valueType
	static Value GetNullValue(ValueType);

	/// Check if the value represents SQL NULL
	bool IsNull() const;

	/// For boolean Values, convert to bool
	bool IsTrue() const;
	bool IsFalse() const;

	/// For boolean Values only, logical operators
	Value OpNegate() const;
	Value OpAnd(const Value rhs) const;
	Value OpOr(const Value rhs) const;

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

	/// Serialize this Value to a SerializeOutput
	void SerializeTo(SerializeOutput &output) const;

	/// Serialize this Value to an Export stream
	void SerializeToExport(ExportSerializeOutput&) const;

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
	static const Value Deserialize(const void *storage,
			const ValueType type, const bool is_inlined);

	/**
	 * Deserialize a scalar value of the specified type from the
	 * SerializeInput directly into the tuple storage area
	 * provided. This function will perform memory allocations for
	 * Object types as necessary using the provided data pool or the
	 * heap. This is used to deserialize tables.
	 * */
	static int64_t DeserializeFrom(SerializeInput &input,
			const ValueType type, char *storage,
			bool is_inlined, const int32_t max_length, Pool *data_pool);

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

	inline bool operator!=(const Value &other) const {
		return !(*this == other);
	}

	/// Return a boolean Value with the comparison result
	Value OpEquals(const Value rhs) const;
	Value OpNotEquals(const Value rhs) const;
	Value OpLessThan(const Value rhs) const;
	Value OpLessThanOrEqual(const Value rhs) const;
	Value OpGreaterThan(const Value rhs) const;
	Value OpGreaterThanOrEqual(const Value rhs) const;

	/// Return a copy of MAX(this, rhs)
	Value OpMax(const Value rhs) const;

	/// Return a copy of MIN(this, rhs)
	Value OpMin(const Value rhs) const;

	/// For number Values, compute new Values for arithmetic operators
	Value OpIncrement() const;
	Value OpDecrement() const;
	Value OpSubtract(const Value rhs) const;
	Value OpAdd(const Value rhs) const;
	Value OpMultiply(const Value rhs) const;
	Value OpDivide(const Value rhs) const;

	/// For number values, check the number line.
	bool IsNegative() const;
	bool IsZero() const;

	//===--------------------------------------------------------------------===//
	// Misc functions
	//===--------------------------------------------------------------------===//

	/// Calculate the tuple storage size for an Value type.
	/// VARCHARs assume out-of-band tuple storage
	static uint16_t GetTupleStorageSize(const ValueType type);

	/// For boost hashing
	void HashCombine(std::size_t &seed) const;

	/// Return a string full of arcane and wonder.
	std::string ToString() const;

	/// Functor comparator for use with std::set
	struct ltValue {
		bool operator()(const Value v1, const Value v2) const {
			return v1.Compare(v2) < 0;
		}
	};

	/// Functor equality predicate for use with boost unordered
	struct equal_to : std::binary_function<Value, Value, bool> {
		bool operator()(Value const& x,
				Value const& y) const
		{
			return x.Compare(y) == 0;
		}
	};

	/// Functor hash predicate for use with boost unordered
	struct hash : std::unary_function<Value, std::size_t> {
		std::size_t operator()(Value const& x) const
		{
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

private:

	//===--------------------------------------------------------------------===//
	// Function declarations for value.cpp
	//===--------------------------------------------------------------------===//

	static std::string GetTypeName(ValueType type);
	void CreateDecimalFromString(const std::string &txt);
	std::string CreateStringFromDecimal() const;
	Value OpDivideDecimals(const Value lhs, const Value rhs) const;
	Value OpMultiplyDecimals(const Value &lhs, const Value &rhs) const;

	/// Promotion Rules.
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
	 * Private constructor that initializes storage and the specifies the type of value
	 * that will be stored in this instance
	 */
	Value(const ValueType type) {
		::memset( value_data, 0, 16);
		SetValueType(type);
		is_inlined = false;
	}

	/**
	 * Set the type of the value that will be stored in this instance.
	 * The last of the 16 bytes of storage allocated in an Value
	 * is used to store the type
	 */
	void SetValueType(ValueType type) {
		value_type = type;
	}

	/**
	 * Get the type of the value. This information is private
	 * to prevent code outside of Value from branching based on the type of a value.
	 */
	ValueType GetValueType() const {
		return value_type;
	}

	void IsInlined(bool sourceInlined){
		is_inlined = sourceInlined;
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
	int32_t GetObjectLength() const {
		if (IsNull()) {
			// Conceptually, I think a NULL object should just have
			// length 0. In practice, this code path is often a defect
			// in code not correctly handling null. May favor a more
			// defensive "return 0" in the future? (rtb)
			throw Exception("Must not ask  for object length on sql null object.");
		}
		if ((GetValueType() != VALUE_TYPE_VARCHAR) && (GetValueType() != VALUE_TYPE_VARBINARY)) {
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

	/**
	 * Retrieve the number of bytes used by the length preceding value
	 * in the object's storage area. This value
	 * is cached in the Value's 13th byte.
	 */
	int8_t GetObjectLengthLength() const {
		return value_data[12];
	}

	/**
	 * Set the objects length preceding values length to
	 * the specified value
	 */
	void SetObjectLengthLength(int8_t length) {
		value_data[12] = length;
	}

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

	/**
	 * Not truly symmetrical with GetObjectValue which returns the actual object past
	 * the length preceding value
	 */
	void SetObjectValue(void* object) {
		*reinterpret_cast<void**>(value_data) = object;
	}

	/**
	 * Get a pointer to the value of an Object that lies beyond the storage of the length information
	 */
	void* GetObjectValue() const {
		if (*reinterpret_cast<void* const*>(value_data) == NULL) {
			return NULL;
		} else if(*reinterpret_cast<const int32_t*>(&value_data[8]) == OBJECTLENGTH_NULL) {
			return NULL;
		} else {
			void* value;
			if (is_inlined)
			{
				value = *reinterpret_cast<char* const*>(value_data) + GetObjectLengthLength();
			}
			else
			{
				Varlen* sref = *reinterpret_cast<Varlen* const*>(value_data);
				value = sref->Get() + GetObjectLengthLength();
			}
			return value;
		}
	}

	//===--------------------------------------------------------------------===//
	// Type Getters
	//===--------------------------------------------------------------------===//

	const int8_t& GetTinyInt() const {
		assert(GetValueType() == VALUE_TYPE_TINYINT);
		return *reinterpret_cast<const int8_t*>(value_data);
	}

	int8_t& GetTinyInt() {
		assert(GetValueType() == VALUE_TYPE_TINYINT);
		return *reinterpret_cast<int8_t*>(value_data);
	}

	const int16_t& GetSmallInt() const {
		assert(GetValueType() == VALUE_TYPE_SMALLINT);
		return *reinterpret_cast<const int16_t*>(value_data);
	}

	int16_t& GetSmallInt() {
		assert(GetValueType() == VALUE_TYPE_SMALLINT);
		return *reinterpret_cast<int16_t*>(value_data);
	}

	const int32_t& GetInteger() const {
		assert(GetValueType() == VALUE_TYPE_INTEGER);
		return *reinterpret_cast<const int32_t*>(value_data);
	}

	int32_t& GetInteger() {
		assert(GetValueType() == VALUE_TYPE_INTEGER);
		return *reinterpret_cast<int32_t*>(value_data);
	}

	const int64_t& GetBigInt() const {
		assert((GetValueType() == VALUE_TYPE_BIGINT) ||
				(GetValueType() == VALUE_TYPE_TIMESTAMP) ||
				(GetValueType() == VALUE_TYPE_ADDRESS));
		return *reinterpret_cast<const int64_t*>(value_data);
	}

	int64_t& GetBigInt() {
		assert((GetValueType() == VALUE_TYPE_BIGINT) ||
				(GetValueType() == VALUE_TYPE_TIMESTAMP) ||
				(GetValueType() == VALUE_TYPE_ADDRESS));
		return *reinterpret_cast<int64_t*>(value_data);
	}

	const int64_t& GetTimestamp() const {
		assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
		return *reinterpret_cast<const int64_t*>(value_data);
	}

	int64_t& GetTimestamp() {
		assert(GetValueType() == VALUE_TYPE_TIMESTAMP);
		return *reinterpret_cast<int64_t*>(value_data);
	}

	const double& GetDouble() const {
		assert(GetValueType() == VALUE_TYPE_DOUBLE);
		return *reinterpret_cast<const double*>(value_data);
	}

	double& GetDouble() {
		assert(GetValueType() == VALUE_TYPE_DOUBLE);
		return *reinterpret_cast<double*>(value_data);
	}

	const bool& GetBoolean() const {
		assert(GetValueType() == VALUE_TYPE_BOOLEAN);
		return *reinterpret_cast<const bool*>(value_data);
	}

	TTInt& GetDecimal() {
		assert(GetValueType() == VALUE_TYPE_DECIMAL);
		void* retval = reinterpret_cast<void*>(value_data);
		return *reinterpret_cast<TTInt*>(retval);
	}

	const TTInt& GetDecimal() const {
		assert(GetValueType() == VALUE_TYPE_DECIMAL);
		const void* retval = reinterpret_cast<const void*>(value_data);
		return *reinterpret_cast<const TTInt*>(retval);
	}

	bool& GetBoolean() {
		assert(GetValueType() == VALUE_TYPE_BOOLEAN);
		return *reinterpret_cast<bool*>(value_data);
	}

	std::size_t GetAllocationSizeForObject() const;
	static std::size_t GetAllocationSizeForObject(int32_t length);

	//===--------------------------------------------------------------------===//
	// Type Cast and Getters
	//===--------------------------------------------------------------------===//

	int64_t CastAsBigIntAndGetValue() const {
		const ValueType type = GetValueType();
		if (IsNull()) {
			return INT64_NULL;
		}

		switch (type) {
		case VALUE_TYPE_NULL:
			return INT64_NULL;
		case VALUE_TYPE_TINYINT:
			return static_cast<int64_t>(GetTinyInt());
		case VALUE_TYPE_SMALLINT:
			return static_cast<int64_t>(GetSmallInt());
		case VALUE_TYPE_INTEGER:
			return static_cast<int64_t>(GetInteger());
		case VALUE_TYPE_ADDRESS:
			return GetBigInt();
		case VALUE_TYPE_BIGINT:
			return GetBigInt();
		case VALUE_TYPE_TIMESTAMP:
			return GetTimestamp();
		case VALUE_TYPE_DOUBLE:
			if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
				throw	ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			return static_cast<int64_t>(GetDouble());
		default:
			throw CastException(type, VALUE_TYPE_BIGINT);
			return 0; // NOT REACHED
		}
	}

	int64_t CastAsRawInt64AndGetValue() const {
		const ValueType type = GetValueType();

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
			throw CastException(type, VALUE_TYPE_BIGINT);
			return 0; // NOT REACHED
		}
	}

	double CastAsDoubleAndGetValue() const {
		const ValueType type = GetValueType();
		if (IsNull()) {
			return DOUBLE_MIN;
		}

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
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
			return 0; // NOT REACHED
		}
	}

	TTInt CastAsDecimalAndGetValue() const {
		const ValueType type = GetValueType();
		if (IsNull()) {
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
			int64_t value = CastAsBigIntAndGetValue();
			TTInt retval(value);
			retval *= Value::max_decimal_scale_factor;
			return retval;
		}
		case VALUE_TYPE_DECIMAL:
			return GetDecimal();
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
			return 0; // NOT REACHED
		}
	}

	Value CastAsBigInt() const {
		Value retval(VALUE_TYPE_BIGINT);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
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
			if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
				throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			retval.GetBigInt() = static_cast<int64_t>(GetDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_BIGINT);
		}
		return retval;
	}

	Value CastAsTimestamp() const {
		Value retval(VALUE_TYPE_TIMESTAMP);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
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
			if (GetDouble() > (double)INT64_MAX || GetDouble() < (double)INT64_MIN) {
				throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_BIGINT);
			}
			retval.GetTimestamp() = static_cast<int64_t>(GetDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_TIMESTAMP);
		}
		return retval;
	}

	Value CastAsInteger() const {
		Value retval(VALUE_TYPE_INTEGER);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.GetInteger() = static_cast<int32_t>(GetTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.GetInteger() = static_cast<int32_t>(GetSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			return *this;
		case VALUE_TYPE_BIGINT:
			if (GetBigInt() > INT32_MAX || GetBigInt() < INT32_MIN) {
				throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_INTEGER);
			}
			retval.GetInteger() = static_cast<int32_t>(GetBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (GetTimestamp() > INT32_MAX || GetTimestamp() < INT32_MIN) {
				throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_INTEGER);
			}
			retval.GetInteger() = static_cast<int32_t>(GetTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (GetDouble() > (double)INT32_MAX || GetDouble() < (double)INT32_MIN) {
				throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_INTEGER);
			}
			retval.GetInteger() = static_cast<int32_t>(GetDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_INTEGER);
		}
		return retval;
	}

	Value CastAsSmallInt() const {
		Value retval(VALUE_TYPE_SMALLINT);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.GetSmallInt() = static_cast<int16_t>(GetTinyInt()); break;
		case VALUE_TYPE_SMALLINT:
			retval.GetSmallInt() = GetSmallInt(); break;
		case VALUE_TYPE_INTEGER:
			if (GetInteger() > INT16_MAX || GetInteger() < INT16_MIN) {
				throw ValueOutOfRangeException((int64_t)GetInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_SMALLINT);
			}
			retval.GetSmallInt() = static_cast<int16_t>(GetInteger()); break;
		case VALUE_TYPE_BIGINT:
			if (GetBigInt() > INT16_MAX || GetBigInt() < INT16_MIN) {
				throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
			}
			retval.GetSmallInt() = static_cast<int16_t>(GetBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (GetTimestamp() > INT16_MAX || GetTimestamp() < INT16_MIN) {
				throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_BIGINT, VALUE_TYPE_SMALLINT);
			}
			retval.GetSmallInt() = static_cast<int16_t>(GetTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (GetDouble() > (double)INT16_MAX || GetDouble() < (double)INT16_MIN) {
				throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_SMALLINT);
			}
			retval.GetSmallInt() = static_cast<int16_t>(GetDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_SMALLINT);
		}
		return retval;
	}

	Value CastAsTinyInt() const {
		Value retval(VALUE_TYPE_TINYINT);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_TINYINT:
			retval.GetTinyInt() = GetTinyInt(); break;
		case VALUE_TYPE_SMALLINT:
			if (GetSmallInt() > INT8_MAX || GetSmallInt() < INT8_MIN) {
				throw ValueOutOfRangeException((int64_t)GetSmallInt(), VALUE_TYPE_SMALLINT, VALUE_TYPE_TINYINT);
			}
			retval.GetTinyInt() = static_cast<int8_t>(GetSmallInt()); break;
		case VALUE_TYPE_INTEGER:
			if (GetInteger() > INT8_MAX || GetInteger() < INT8_MIN) {
				throw ValueOutOfRangeException((int64_t)GetInteger(), VALUE_TYPE_INTEGER, VALUE_TYPE_TINYINT);
			}
			retval.GetTinyInt() = static_cast<int8_t>(GetInteger()); break;
		case VALUE_TYPE_BIGINT:
			if (GetBigInt() > INT8_MAX || GetBigInt() < INT8_MIN) {
				throw ValueOutOfRangeException(GetBigInt(), VALUE_TYPE_BIGINT, VALUE_TYPE_TINYINT);
			}
			retval.GetTinyInt() = static_cast<int8_t>(GetBigInt()); break;
		case VALUE_TYPE_TIMESTAMP:
			if (GetTimestamp() > INT8_MAX || GetTimestamp() < INT8_MIN) {
				throw ValueOutOfRangeException(GetTimestamp(), VALUE_TYPE_TIMESTAMP, VALUE_TYPE_TINYINT);
			}
			retval.GetTinyInt() = static_cast<int8_t>(GetTimestamp()); break;
		case VALUE_TYPE_DOUBLE:
			if (GetDouble() > (double)INT8_MAX || GetDouble() < (double)INT8_MIN) {
				throw ValueOutOfRangeException(GetDouble(), VALUE_TYPE_DOUBLE, VALUE_TYPE_TINYINT);
			}
			retval.GetTinyInt() = static_cast<int8_t>(GetDouble()); break;
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_TINYINT);
		}
		return retval;
	}

	Value CastAsDouble() const {
		Value retval(VALUE_TYPE_DOUBLE);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
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
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
		case VALUE_TYPE_DECIMAL:
		default:
			throw	CastException(type, VALUE_TYPE_DOUBLE);
		}
		return retval;
	}

	Value CastAsString() const {
		Value retval(VALUE_TYPE_VARCHAR);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
		// note: we allow binary conversion to strings to support
		// byte[] as string parameters...
		// In the future, it would be nice to check this is a decent string here...
		switch (type) {
		case VALUE_TYPE_VARCHAR:
		case VALUE_TYPE_VARBINARY:
			memcpy(retval.value_data, value_data, sizeof(value_data));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_VARCHAR);
		}
		return retval;
	}

	Value CastAsBinary() const {
		Value retval(VALUE_TYPE_VARBINARY);
		const ValueType type = GetValueType();
		if (IsNull()) {
			retval.SetNull();
			return retval;
		}
		switch (type) {
		case VALUE_TYPE_VARBINARY:
			memcpy(retval.value_data, value_data, sizeof(value_data));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_VARBINARY);
		}
		return retval;
	}

	Value CastAsDecimal() const {
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
			TTInt retval(rhsint);
			retval *= Value::max_decimal_scale_factor;
			return GetDecimalValue(retval);
		}
		case VALUE_TYPE_DECIMAL:
			::memcpy(retval.value_data, value_data, sizeof(TTInt));
			break;
		default:
			throw	CastException(type, VALUE_TYPE_DECIMAL);
		}
		return retval;
	}

	//===--------------------------------------------------------------------===//
	// Type Comparison
	//===--------------------------------------------------------------------===//

	int CompareAnyIntegerValue (const Value rhs) const {
		int64_t lhsValue, rhsValue;

		// Get the right hand side as a bigint
		if (rhs.GetValueType() != VALUE_TYPE_BIGINT) rhsValue = rhs.CastAsBigIntAndGetValue();
		else rhsValue = rhs.GetBigInt();

		// convert the left hand side
		switch(GetValueType()) {
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_TIMESTAMP:
			lhsValue = CastAsBigIntAndGetValue(); break;
		case VALUE_TYPE_BIGINT:
			lhsValue = GetBigInt(); break;
		default: {
			throw TypeMismatchException("non comparable types lhs '%d' rhs '%d'", GetValueType(), rhs.GetValueType());
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

	int CompareDoubleValue (const Value rhs) const {
		switch (rhs.GetValueType()) {
		case VALUE_TYPE_DOUBLE: {
			const double lhsValue = GetDouble();
			const double rhsValue = rhs.GetDouble();
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
			const double lhsValue = GetDouble();
			const double rhsValue = rhs.CastAsDouble().GetDouble();
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
			double val = rhs.CastAsDoubleAndGetValue();
			if (rhs.IsNegative()) {
				val *= -1;
			}
			return ((GetDouble() > val) - (GetDouble() < val));
		}
		default:
		{
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be Cast for comparison to type %s",
					ValueToString(rhs.GetValueType()).c_str(),
					ValueToString(GetValueType()).c_str());
			throw Exception(message);
			// Not reached
			return 0;
		}
		}
	}

	int CompareStringValue (const Value rhs) const {
		if ((rhs.GetValueType() != VALUE_TYPE_VARCHAR) && (rhs.GetValueType() != VALUE_TYPE_VARBINARY)) {
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be Cast for comparison to type %s",
					ValueToString(rhs.GetValueType()).c_str(),
					ValueToString(GetValueType()).c_str());
			throw Exception(message);
		}
		const char* left = reinterpret_cast<const char*>(GetObjectValue());
		const char* right = reinterpret_cast<const char*>(rhs.GetObjectValue());
		if (IsNull()) {
			if (rhs.IsNull()) {
				return VALUE_COMPARE_EQUAL;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		} else if (rhs.IsNull()) {
			return VALUE_COMPARE_GREATERTHAN;
		}
		const int32_t leftLength = GetObjectLength();
		const int32_t rightLength = rhs.GetObjectLength();
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
		if (rhs.GetValueType() != VALUE_TYPE_VARBINARY) {
			char message[128];
			snprintf(message, 128,
					"Type %s cannot be Cast for comparison to type %s",
					ValueToString(rhs.GetValueType()).c_str(),
					ValueToString(GetValueType()).c_str());
			throw Exception(message);
		}
		const char* left = reinterpret_cast<const char*>(GetObjectValue());
		const char* right = reinterpret_cast<const char*>(rhs.GetObjectValue());
		if (IsNull()) {
			if (rhs.IsNull()) {
				return VALUE_COMPARE_EQUAL;
			} else {
				return VALUE_COMPARE_LESSTHAN;
			}
		} else if (rhs.IsNull()) {
			return VALUE_COMPARE_GREATERTHAN;
		}
		const int32_t leftLength = GetObjectLength();
		const int32_t rightLength = rhs.GetObjectLength();
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

	int CompareDecimalValue(const Value rhs) const {
		switch (rhs.GetValueType()) {
		// create the equivalent decimal value
		case VALUE_TYPE_TINYINT:
		case VALUE_TYPE_SMALLINT:
		case VALUE_TYPE_INTEGER:
		case VALUE_TYPE_BIGINT:
		{
			const TTInt lhsValue = GetDecimal();
			const TTInt rhsValue = rhs.CastAsDecimalAndGetValue();

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
			const TTInt lhsValue = GetDecimal();
			const TTInt rhsValue = rhs.GetDecimal();

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
			const double lhsValue = CastAsDoubleAndGetValue();
			const double rhsValue = rhs.GetDouble();

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
					"Type %s cannot be Cast for comparison to type %s",
					ValueToString(rhs.GetValueType()).c_str(),
					ValueToString(GetValueType()).c_str());
			throw	TypeMismatchException(message, GetValueType(), rhs.GetValueType());
			// Not reached
			return 0;
		}
		}
	}

	//===--------------------------------------------------------------------===//
	// Type Operators
	//===--------------------------------------------------------------------===//

	Value OpAddBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return GetBigIntValue(INT64_NULL);
		//Scary overflow check
		if ( ((lhs^rhs)
				| (((lhs^(~(lhs^rhs)
						& (1L << (sizeof(int64_t)*CHAR_BIT-1))))+rhs)^rhs)) >= 0) {
			char message[4096];
			snprintf(message, 4096, "Adding %jd and %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetBigIntValue(lhs + rhs);
	}

	Value OpSubtractBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return GetBigIntValue(INT64_NULL);
		//Scary overflow check
		if ( ((lhs^rhs)
				& (((lhs ^ ((lhs^rhs)
						& (1L << (sizeof(int64_t)*CHAR_BIT-1))))-rhs)^rhs)) < 0) {
			char message[4096];
			snprintf(message, 4096, "Subtracting %jd from %jd will overflow BigInt storage", (intmax_t)lhs, (intmax_t)rhs);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetBigIntValue(lhs - rhs);
	}

	Value OpMultiplyBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return GetBigIntValue(INT64_NULL);
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

		return GetBigIntValue(result);
	}

	Value OpDivideBigInts(const int64_t lhs, const int64_t rhs) const {
		if (lhs == INT64_NULL || rhs == INT64_NULL)
			return GetBigIntValue(INT64_NULL);

		if (rhs == 0) {
			char message[4096];
			snprintf(message, 4096, "Attempted to divide %jd by 0", (intmax_t)lhs);
			throw	DivideByZeroException(message);
		}

		/**
		 * Because the smallest int64 value is used to represent null (and this is checked for an handled above)
		 * it isn't necessary to check for any kind of overflow since none is possible.
		 */
		return GetBigIntValue(int64_t(lhs / rhs));
	}

	Value OpAddDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return GetDoubleValue(DOUBLE_MIN);

		const double result = lhs + rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to add %f with %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetDoubleValue(result);
	}

	Value OpSubtractDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return GetDoubleValue(DOUBLE_MIN);

		const double result = lhs - rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to subtract %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetDoubleValue(result);
	}

	Value OpMultiplyDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return GetDoubleValue(DOUBLE_MIN);

		const double result = lhs * rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to multiply %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetDoubleValue(result);
	}

	Value OpDivideDoubles(const double lhs, const double rhs) const {
		if (lhs <= DOUBLE_NULL || rhs <= DOUBLE_NULL)
			return GetDoubleValue(DOUBLE_MIN);


		const double result = lhs / rhs;

		if (CHECK_FPE(result)) {
			char message[4096];
			snprintf(message, 4096, "Attempted to divide %f by %f caused overflow/underflow or some other error. Result was %f",
					lhs, rhs, result);
			throw	NumericValueOutOfRangeException(message);
		}
		return GetDoubleValue(result);
	}

	Value OpAddDecimals(const Value lhs, const Value rhs) const {
		if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) ||
				(rhs.GetValueType() != VALUE_TYPE_DECIMAL))
		{
			throw Exception("Non-decimal Value in decimal adder.");
		}

		if (lhs.IsNull() || rhs.IsNull()) {
			TTInt retval;
			retval.SetMin();
			return GetDecimalValue(retval);
		}

		TTInt retval(lhs.GetDecimal());
		if (retval.Add(rhs.GetDecimal()) || retval > Value::MaxDecimal || retval < MinDecimal) {
			char message[4096];
			snprintf(message, 4096, "Attempted to add %s with %s causing overflow/underflow",
					lhs.CreateStringFromDecimal().c_str(), rhs.CreateStringFromDecimal().c_str());
			throw	NumericValueOutOfRangeException(message);
		}

		return GetDecimalValue(retval);
	}

	Value OpSubtractDecimals(const Value lhs, const Value rhs) const {
		if ((lhs.GetValueType() != VALUE_TYPE_DECIMAL) ||
				(rhs.GetValueType() != VALUE_TYPE_DECIMAL))
		{
			throw Exception("Non-decimal Value in decimal subtract.");
		}

		if (lhs.IsNull() || rhs.IsNull()) {
			TTInt retval;
			retval.SetMin();
			return GetDecimalValue(retval);
		}

		TTInt retval(lhs.GetDecimal());
		if (retval.Sub(rhs.GetDecimal()) || retval > Value::MaxDecimal || retval < Value::MinDecimal) {
			char message[4096];
			snprintf(message, 4096, "Attempted to subtract %s from %s causing overflow/underflow",
					rhs.CreateStringFromDecimal().c_str(), lhs.CreateStringFromDecimal().c_str());
			throw	NumericValueOutOfRangeException(message);
		}

		return GetDecimalValue(retval);
	}

	//===--------------------------------------------------------------------===//
	// Static Type Getters
	//===--------------------------------------------------------------------===//

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
		Varlen* sref = Varlen::Create(minLength, data_pool);
		char* storage = sref->Get();
		SetObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, value.c_str(), length);
		retval.SetObjectValue(sref);
		retval.SetObjectLength(length);
		retval.SetObjectLengthLength(lengthLength);
		return retval;
	}

	/// Assumes binary value in hex
	static Value GetBinaryValue(const std::string value, Pool *data_pool) {
		Value retval(VALUE_TYPE_VARBINARY);
		const int32_t length = static_cast<int32_t>(value.length() / 2);

		boost::shared_array<unsigned char> buf(new unsigned char[length]);
		HexDecodeToBinary(buf.get(), value.c_str());

		const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
		const int32_t minLength = length + lengthLength;
		Varlen* sref = Varlen::Create(minLength, data_pool);
		char* storage = sref->Get();
		SetObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, buf.get(), length);
		retval.SetObjectValue(sref);
		retval.SetObjectLength(length);
		retval.SetObjectLengthLength(lengthLength);
		return retval;
	}

	static Value GetBinaryValue(const unsigned char *value, const int32_t length, Pool *data_pool) {
		Value retval(VALUE_TYPE_VARBINARY);
		const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
		const int32_t minLength = length + lengthLength;
		Varlen* sref = Varlen::Create(minLength, data_pool);
		char* storage = sref->Get();
		SetObjectLengthToLocation(length, storage);
		::memcpy( storage + lengthLength, value, length);
		retval.SetObjectValue(sref);
		retval.SetObjectLength(length);
		retval.SetObjectLengthLength(lengthLength);
		return retval;
	}

	static Value GetNullStringValue() {
		Value retval(VALUE_TYPE_VARCHAR);
		*reinterpret_cast<char**>(retval.value_data) = NULL;
		return retval;
	}

	static Value GetNullBinaryValue() {
		Value retval(VALUE_TYPE_VARBINARY);
		*reinterpret_cast<char**>(retval.value_data) = NULL;
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
		*reinterpret_cast<void**>(retval.value_data) = address;
		return retval;
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
			*reinterpret_cast<char*>(storage) = OBJECT_NULL_BIT;
		}
		else {
			const int32_t objectLength = GetObjectLength();
			if (objectLength > max_length) {
				char msg[1024];
				snprintf(msg, 1024, "Object exceeds specified size. Size is %d and max is %d", objectLength, max_length);
				throw ObjectSizeException(msg);
			}
			if (is_inlined)
			{
				::memcpy( storage, *reinterpret_cast<char *const *>(value_data), GetObjectLengthLength() + objectLength);
			}
			else
			{
				const Varlen* sref =
						*reinterpret_cast<Varlen* const*>(value_data);
				::memcpy(storage, sref->Get(),
						GetObjectLengthLength() + objectLength);
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
	switch (GetValueType()){
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		assert(!is_inlined);
		Varlen* sref = *reinterpret_cast<Varlen* const*>(value_data);
		if (sref != NULL)
		{
			Varlen::Destroy(sref);
		}
	}
	break;
	default:
		return;
	}
}

inline Value Value::CastAs(ValueType type) const {
	if (GetValueType() == type) {
		return *this;
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
		snprintf(message, 128, "Type %d not a recognized type for Casting",
				(int) type);

		throw TypeMismatchException(message, GetValueType(), type);
	}
}

inline void* Value::CastAsAddress() const {
	const ValueType type = GetValueType();
	switch (type) {
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_ADDRESS:
		return *reinterpret_cast<void* const*>(value_data);
	default:
		throw UnknownTypeException((int) type, "Type %d not a recognized type for casting as an address");
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
		throw UnknownTypeException(type, "Invalid value type for checking negativity");
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
	if(GetBoolean() || rhs.GetBoolean()) {
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
		return *reinterpret_cast<void* const*>(value_data) == NULL;
	case VALUE_TYPE_DOUBLE:
		return GetDouble() <= DOUBLE_NULL;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		return (*reinterpret_cast<void* const*>(value_data) == nullptr ||
				*reinterpret_cast<const int32_t*>(&value_data[8]) == OBJECTLENGTH_NULL);
	case VALUE_TYPE_DECIMAL: {
		TTInt min;
		min.SetMin();
		return GetDecimal() == min;
	}
	default:
		throw IncompatibleTypeException(GetValueType(), "Value::isNull() called with ValueType '%d'");
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
		*reinterpret_cast<void**>(value_data) = NULL;
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
		return sizeof(char*);
	case VALUE_TYPE_DECIMAL:
		return sizeof(TTInt);
	default:
		char message[128];
		snprintf(message, 128, "unrecognized type %d", type);
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
		throw IncompatibleTypeException(rhs.GetValueType(), "non comparable type '%d'");
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
inline void Value::Serialize(void *storage, const bool is_inlined, const int32_t max_length) const{
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
		::memcpy( storage, value_data, Value::GetTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		//Potentially non-inlined type requires special handling
		if (is_inlined) {
			InlineCopyObject(storage, max_length);
		}
		else {
			if (IsNull() || GetObjectLength() <= max_length) {
				if (is_inlined && !is_inlined)
				{
					throw Exception("Cannot serialize an inlined string to non-inlined tuple storage in serializeToTupleStorage()");
				}
				// copy the StringRef pointers
				*reinterpret_cast<Varlen**>(storage) =
						*reinterpret_cast<Varlen* const*>(value_data);
			}
			else {
				const int32_t length = GetObjectLength();
				char msg[1024];
				snprintf(msg, 1024, "Object exceeds specified size. Size is %d and max is %d", length, max_length);
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
		const int32_t length = GetObjectLength();
		if (length < OBJECTLENGTH_NULL) {
			throw Exception("Attempted to serialize an Value with a negative length");
		}
		output.WriteInt(static_cast<int32_t>(length));
		if (length != OBJECTLENGTH_NULL) {
			// Not a null string: write it out
			const char * str = reinterpret_cast<const char*>(GetObjectValue());
			if (str == NULL) {}
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
		throw UnknownTypeException(type, "Value::serializeTo() found a column "
				"with ValueType '%d' that is not handled");
	}
}

inline void Value::SerializeToExport(ExportSerializeOutput &io) const
{
	switch (GetValueType()) {
	case VALUE_TYPE_TINYINT:
	case VALUE_TYPE_SMALLINT:
	case VALUE_TYPE_INTEGER:
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
	{
		int64_t val = CastAsBigIntAndGetValue();
		io.WriteLong(val);
		return;
	}
	case VALUE_TYPE_DOUBLE:
	{
		double value = GetDouble();
		io.WriteDouble(value);
		return;
	}
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		// requires (and uses) bytecount not character count
		io.WriteBinaryString(GetObjectValue(), GetObjectLength());
		return;
	}
	case VALUE_TYPE_DECIMAL:
	{
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
		snprintf(message, 128, "Invalid type in serializeToExport: %d", GetValueType());
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
		const int32_t max_length, Pool *data_pool) const{
	const ValueType type = GetValueType();
	int32_t length = 0;

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
		::memcpy( storage, value_data, Value::GetTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
		//Potentially non-inlined type requires special handling
		if (is_inlined) {
			InlineCopyObject(storage, max_length);
		}
		else {
			if (IsNull()) {
				*reinterpret_cast<void**>(storage) = NULL;
			}
			else {
				length = GetObjectLength();
				const int8_t lengthLength = GetObjectLengthLength();
				const int32_t minlength = lengthLength + length;
				if (length > max_length) {
					char msg[1024];
					snprintf(msg, 1024, "Object exceeds specified size. Size is %d"
							" and max is %d", length, max_length);
					throw ObjectSizeException(msg);
				}
				Varlen* sref = Varlen::Create(minlength, data_pool);
				char *copy = sref->Get();
				SetObjectLengthToLocation(length, copy);
				::memcpy(copy + lengthLength, GetObjectValue(), length);
				*reinterpret_cast<Varlen**>(storage) = sref;
			}
		}
		break;
	default: {
		throw UnknownTypeException(type, "unrecognized type '%d'");
	}
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
inline const Value Value::Deserialize(const void *storage,
		const ValueType type,
		const bool is_inlined){
	Value retval(type);
	switch (type)
	{
	case VALUE_TYPE_TIMESTAMP:
		retval.GetTimestamp() = *reinterpret_cast<const int64_t*>(storage);
		break;
	case VALUE_TYPE_TINYINT:
		retval.GetTinyInt() = *reinterpret_cast<const int8_t*>(storage);
		break;
	case VALUE_TYPE_SMALLINT:
		retval.GetSmallInt() = *reinterpret_cast<const int16_t*>(storage);
		break;
	case VALUE_TYPE_INTEGER:
		retval.GetInteger() = *reinterpret_cast<const int32_t*>(storage);
		break;
	case VALUE_TYPE_BIGINT:
		retval.GetBigInt() = *reinterpret_cast<const int64_t*>(storage);
		break;
	case VALUE_TYPE_DOUBLE:
		retval.GetDouble() = *reinterpret_cast<const double*>(storage);
		break;
	case VALUE_TYPE_DECIMAL:
		::memcpy( retval.value_data, storage, Value::GetTupleStorageSize(type));
		break;
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		//Potentially non-inlined type requires special handling
		char* data = NULL;

		if (is_inlined) {
			//If it is inlined the storage area contains the actual data so copy a reference
			//to the storage area
			*reinterpret_cast<void**>(retval.value_data) = const_cast<void*>(storage);
			data = *reinterpret_cast<char**>(retval.value_data);
			retval.IsInlined(true);
		} else {
			//If it isn't inlined the storage area contains a pointer to the
			// StringRef object containing the string's memory
			memcpy( retval.value_data, storage, sizeof(void*));
			Varlen* sref = nullptr;
			sref = *reinterpret_cast<Varlen**>(retval.value_data);

			// If the StringRef pointer is null, that's because this
			// was a null value; leave the data pointer as NULL so
			// that GetObjectLengthFromLocation will figure this out
			// correctly, otherwise Get the right char* from the StringRef
			if (sref != NULL)
			{
				data = sref->Get();
			}
		}

		const int32_t length = GetObjectLengthFromLocation(data);
		//std::cout << "Value::deserializeFromTupleStorage: length: " << length << std::endl;
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
inline int64_t Value::DeserializeFrom(SerializeInput &input, const ValueType type,
		char *storage, bool is_inlined, const int32_t max_length, Pool *data_pool) {
	switch (type) {
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		*reinterpret_cast<int64_t*>(storage) = input.ReadLong();
		return sizeof(int64_t);
	case VALUE_TYPE_TINYINT:
		*reinterpret_cast<int8_t*>(storage) = input.ReadByte();
		return sizeof(int8_t);
	case VALUE_TYPE_SMALLINT:
		*reinterpret_cast<int16_t*>(storage) = input.ReadShort();
		return sizeof(int16_t);
	case VALUE_TYPE_INTEGER:
		*reinterpret_cast<int32_t*>(storage) = input.ReadInt();
		return sizeof(int32_t);
	case VALUE_TYPE_DOUBLE:
		*reinterpret_cast<double* >(storage) = input.ReadDouble();
		return sizeof(double);
	case VALUE_TYPE_VARCHAR:
	case VALUE_TYPE_VARBINARY:
	{
		int64_t bytesRead = 0;
		const int32_t length = input.ReadInt();
		bytesRead+= sizeof(int32_t);
		if (length > max_length) {
			char msg[1024];
			snprintf(msg, 1024, "String exceeds specified size. Size is %d and max is %d", length, max_length);
			throw ObjectSizeException(msg);
		}

		const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
		// the NULL SQL string is a NULL C pointer
		if (is_inlined) {
			SetObjectLengthToLocation(length, storage);
			if (length == OBJECTLENGTH_NULL) {
				return 0;
			}
			const char *data = reinterpret_cast<const char*>(input.GetRawPointer(length));
			::memcpy( storage + lengthLength, data, length);
		} else {
			if (length == OBJECTLENGTH_NULL) {
				*reinterpret_cast<void**>(storage) = NULL;
				return 0;
			}
			const char *data = reinterpret_cast<const char*>(input.GetRawPointer(length));
			const int32_t minlength = lengthLength + length;
			Varlen* sref = Varlen::Create(minlength, data_pool);
			char* copy = sref->Get();
			SetObjectLengthToLocation( length, copy);
			::memcpy(copy + lengthLength, data, length);
			*reinterpret_cast<Varlen**>(storage) = sref;
		}
		bytesRead+=length;
		return bytesRead;
	}
	case VALUE_TYPE_DECIMAL: {
		int64_t *longStorage = reinterpret_cast<int64_t*>(storage);
		//Reverse order for Java BigDecimal BigEndian
		longStorage[1] = input.ReadLong();
		longStorage[0] = input.ReadLong();
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
inline const Value Value::DeserializeWithAllocation(SerializeInput &input, Pool *data_pool) {
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
	case VALUE_TYPE_VARBINARY:
	{
		const int32_t length = input.ReadInt();
		const int8_t lengthLength = GetAppropriateObjectLengthLength(length);
		// the NULL SQL string is a NULL C pointer
		if (length == OBJECTLENGTH_NULL) {
			retval.SetNull();
			break;
		}
		const void *str = input.GetRawPointer(length);
		const int32_t minlength = lengthLength + length;
		Varlen* sref = Varlen::Create(minlength, data_pool);
		char* copy = sref->Get();
		retval.SetObjectLengthToLocation( length, copy);
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
		throw UnknownTypeException(type, "Value::deserializeFromAllocateForStorage() unrecognized type '%d'");
	}
	return retval;
}

//===--------------------------------------------------------------------===//
// Type operators
//===--------------------------------------------------------------------===//


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

inline Value Value::OpMax(const Value rhs) const {
	const int value = Compare(rhs);
	if (value > 0) {
		return *this;
	} else {
		return rhs;
	}
}

inline Value Value::OpMin(const Value rhs) const {
	const int value = Compare(rhs);
	if (value < 0) {
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
		throw IncompatibleTypeException ((int) type, "type %d is not a numeric type that implements isZero()");
	}
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
		boost::hash_combine( seed, GetDouble()); break;
	case VALUE_TYPE_VARCHAR: {
		if (GetObjectValue() == NULL) {
			boost::hash_combine( seed, std::string(""));
		} else {
			const int32_t length = GetObjectLength();
			boost::hash_combine( seed, std::string( reinterpret_cast<const char*>(GetObjectValue()), length ));
		}
		break;
	}
	case VALUE_TYPE_VARBINARY: {
		if (GetObjectValue() == NULL) {
			boost::hash_combine( seed, std::string(""));
		} else {
			const int32_t length = GetObjectLength();
			char* data = reinterpret_cast<char*>(GetObjectValue());
			for (int32_t i = 0; i < length; i++)
				boost::hash_combine(seed, data[i]);
		}
		break;
	}
	case VALUE_TYPE_DECIMAL:
		GetDecimal().hash(seed); break;
	default:
		throw UnknownTypeException((int) type, "unknown type %d");
	}
}

inline Value Value::OpIncrement() const {
	const ValueType type = GetValueType();
	Value retval(type);
	switch(type) {
	case VALUE_TYPE_TINYINT:
		if (GetTinyInt() == INT8_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this TinyInt results in a value out of range");
		}
		retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() + 1); break;
	case VALUE_TYPE_SMALLINT:
		if (GetSmallInt() == INT16_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this SmallInt results in a value out of range");
		}
		retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() + 1); break;
	case VALUE_TYPE_INTEGER:
		if (GetInteger() == INT32_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this Integer results in a value out of range");
		}
		retval.GetInteger() = GetInteger() + 1; break;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		if (GetBigInt() == INT64_MAX) {
			throw NumericValueOutOfRangeException("Incrementing this BigInt/Timestamp results in a value out of range");
		}
		retval.GetBigInt() = GetBigInt() + 1; break;
	case VALUE_TYPE_DOUBLE:
		retval.GetDouble() = GetDouble() + 1; break;
	default:
		throw IncompatibleTypeException((int) type, "type %d is not incrementable");
		break;
	}
	return retval;
}

inline Value Value::OpDecrement() const {
	const ValueType type = GetValueType();
	Value retval(type);
	switch(type) {
	case VALUE_TYPE_TINYINT:
		if (GetTinyInt() == INT8_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this TinyInt results in a value out of range");
		}
		retval.GetTinyInt() = static_cast<int8_t>(GetTinyInt() - 1); break;
	case VALUE_TYPE_SMALLINT:
		if (GetSmallInt() == INT16_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this SmallInt results in a value out of range");
		}
		retval.GetSmallInt() = static_cast<int16_t>(GetSmallInt() - 1); break;
	case VALUE_TYPE_INTEGER:
		if (GetInteger() == INT32_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this Integer results in a value out of range");
		}
		retval.GetInteger() = GetInteger() - 1; break;
	case VALUE_TYPE_BIGINT:
	case VALUE_TYPE_TIMESTAMP:
		if (GetBigInt() == INT64_MIN) {
			throw NumericValueOutOfRangeException("Decrementing this BigInt/Timestamp results in a value out of range");
		}
		retval.GetBigInt() = GetBigInt() - 1; break;
	case VALUE_TYPE_DOUBLE:
		retval.GetDouble() = GetDouble() - 1; break;
	default:
		throw IncompatibleTypeException ((int) type, "type %d is not decrementable");
		break;
	}
	return retval;
}

inline Value Value::OpSubtract(const Value rhs) const {
	ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
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
	throw TypeMismatchException("Promotion of %s and %s failed in Opsubtract.",
			GetValueType(),
			rhs.GetValueType());
}

inline Value Value::OpAdd(const Value rhs) const {
	ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
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
	throw TypeMismatchException("Promotion of %s and %s failed in Opadd.",
			GetValueType(),
			rhs.GetValueType());
}

inline Value Value::OpMultiply(const Value rhs) const {
	ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
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
		return OpMultiplyDecimals(*this, rhs);

	default:
		break;
	}
	throw TypeMismatchException("Promotion of %s and %s failed in Opmultiply.",
			GetValueType(),
			rhs.GetValueType());
}

inline Value Value::OpDivide(const Value rhs) const {
	ValueType vt = PromoteForOp(GetValueType(), rhs.GetValueType());
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

	throw TypeMismatchException("Promotion of %s and %s failed in Opdivide.",
			GetValueType(),
			rhs.GetValueType());
}



} // End nstore namespace


