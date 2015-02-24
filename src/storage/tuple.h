/*-------------------------------------------------------------------------
 *
 * abstract_tuple.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/abstract_tuple.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <memory>

#include "catalog/tuple_schema.h"
#include "common/value.h"
#include "common/value_peeker.h"
#include "common/types.h"

namespace nstore {
namespace storage {

//===--------------------------------------------------------------------===//
// Tuple class
//===--------------------------------------------------------------------===//

/**
 *
 * TUPLE LAYOUT
 *
 *	In Primary Tile ::
 * 	-----------------------------------------------------------------------------------
 * 	| Flags (1 byte) | CTS (4 bytes) | ETS (4 bytes) | Next ((4 bytes) | tuple data   |
 * 	-----------------------------------------------------------------------------------
 *
 * 	In Other Tile(s) ::
 * 	----------------
 * 	| tuple data   |
 * 	----------------
 *
 **/

#define TUPLE_HEADER_SIZE 1

#define DELETED_MASK 1
#define DIRTY_MASK 2
#define MIGRATED_MASK 4

class Tuple {
	friend class catalog::TupleSchema;
	friend class ValuePeeker;

	class TableColumn;

public:

	/// Default constructor (don't use this)
	inline Tuple() : tuple_schema(NULL), tuple_data(NULL) {
	}

	/// Setup the tuple given a table
	inline Tuple(const Tuple &rhs) : tuple_schema(rhs.tuple_schema), tuple_data(rhs.tuple_data) {
	}

	/// Setup the tuple given a schema
	inline Tuple(std::shared_ptr<catalog::TupleSchema> schema) : tuple_schema(schema), tuple_data(NULL) {
		assert(tuple_schema);
	}

	/// Setup the tuple given a schema and allocate space
	inline Tuple(std::shared_ptr<catalog::TupleSchema> schema, bool allocate) : tuple_schema(schema) {
		assert(tuple_schema);
		if(allocate)
			tuple_data = new char[tuple_schema->GetLength()];
	}

	/// Deletes tuple data (not schema)
	~Tuple() {
		// first free all uninlined data
		if(tuple_schema && tuple_schema->IsInlined() == false)
			FreeColumns();

		/// then delete the actual data
		delete tuple_data;
	}

	/// Setup the tuple given the specified data location and schema
	Tuple(char *data, std::shared_ptr<catalog::TupleSchema> schema);

	/// Assignment operator
	Tuple& operator=(const Tuple &rhs);

	// Only release the space taken up by columns
	inline void FreeColumns();

	/// Copy values from one tuple into another (uses memcpy)
	/// (expensive) verify assumptions for copy
	bool CompatibleForCopy(const Tuple &source);

	void Copy(const Tuple &source);
	void CopyForInsert(const Tuple &source, Pool *pool = NULL);
	void CopyForUpdate(const Tuple &source, Pool *pool = NULL);

	bool operator==(const Tuple &other) const;
	bool operator!=(const Tuple &other) const;

	int Compare(const Tuple &other) const;

	//===--------------------------------------------------------------------===//
	// Getters and Setters
	//===--------------------------------------------------------------------===//

	/// Get the value of a specified column (const)
	/// (expensive) checks the schema to see how to return the Value.
	inline const Value GetValue(const int column_id) const {
		assert(tuple_schema);
		assert(tuple_data);

		assert(column_id < tuple_schema->GetColumnCount());
		//assert(IsAlive());

		const ValueType column_type = tuple_schema->GetType(column_id);

		const char* data_ptr = GetDataPtr(column_id);
		const bool is_inlined = tuple_schema->IsInlined(column_id);

		return Value::Deserialize(data_ptr, column_type, is_inlined);
	}

	/// Set appropriate column in tuple
	void SetValue(const int column_id, Value value);

	/**
	 * Allocate space to copy strings that can't be inlined rather
	 * than copying the pointer.
	 * Used when setting a SlimValue that will go into
	 * permanent storage in a persistent table.  It is also possible
	 * to provide NULL for stringPool in which case the strings will
	 * be allocated on the heap.
	 */
	void SetValueAllocateForObjectCopies(const int column_id, Value value,
			Pool *dataPool);

	inline int Length() const {
		return tuple_schema->GetLength() + TUPLE_HEADER_SIZE;
	}

	/// Is the tuple dead or alive ?
	inline bool IsAlive() const {
		return (*(reinterpret_cast<const char*>(tuple_data)) & DELETED_MASK)
				== 0 ? true : false;
	}

	/// Is the column value null ?
	inline bool IsNull(const int column_id) const {
		return GetValue(column_id).IsNull();
	}

	/// Is the tuple null ?
	inline bool IsNull() const {
		return tuple_data == NULL;
	}

	/// Get the type of a particular column in the tuple
	inline ValueType GetType(int column_id) const {
		return tuple_schema->GetType(column_id);
	}

	inline std::shared_ptr<catalog::TupleSchema> GetSchema() const {
		return tuple_schema;
	}

	/// Get the address of this tuple in the table's backing store
	inline char* Location() const {
		return tuple_data;
	}

	/// Return the number of columns in this tuple
	inline int GetColumnCount() const {
		return tuple_schema->GetColumnCount();
	}

	bool EqualsNoSchemaCheck(const Tuple &other) const;

	/// this does set NULL in addition to clear string count.
	void SetAllNulls();

	/**
	 * Determine the maximum number of bytes when serialized for Export.
	 * Excludes the bytes required by the row header (which includes
	 * the null bit indicators) and ignores the width of metadata columns.
	 */
	size_t ExportSerializationSize() const {
		size_t bytes = 0;
		int column_count = GetColumnCount();

		for (int column_itr = 0; column_itr < column_count; ++column_itr) {
			switch (GetType(column_itr)) {
			case VALUE_TYPE_TINYINT:
			case VALUE_TYPE_SMALLINT:
			case VALUE_TYPE_INTEGER:
			case VALUE_TYPE_BIGINT:
			case VALUE_TYPE_TIMESTAMP:
			case VALUE_TYPE_DOUBLE:
				bytes += sizeof(int64_t);
				break;

			case VALUE_TYPE_DECIMAL:
				// Decimals serialized in ascii as
				// 32 bits of length + max prec digits + radix pt + sign
				bytes += sizeof(int32_t) + Value::max_decimal_precision + 1 + 1;
				break;

			case VALUE_TYPE_VARCHAR:
			case VALUE_TYPE_VARBINARY:
				// 32 bit length preceding value and
				// actual character data without null string terminator.
				if (!GetValue(column_itr).IsNull()) {
					bytes += (sizeof(int32_t)
							+ ValuePeeker::PeekObjectLength(
									GetValue(column_itr)));
				}
				break;

			default:
				throw UnknownTypeException(GetType(column_itr),
						"Unknown ValueType found during Export serialization.");
				return (size_t) 0;
			}
		}
		return bytes;
	}

	/// Return the amount of memory allocated for non-inlined objects
	size_t GetUninlinedMemorySize() const {
		size_t bytes = 0;
		int column_count = GetColumnCount();

		// fast-path for no inlined cols
		if (tuple_schema->IsInlined() == false) {
			for (int column_itr = 0; column_itr < column_count; ++column_itr) {
				// peekObjectLength is unhappy with non-varchar
				if ((GetType(column_itr) == VALUE_TYPE_VARCHAR
						|| (GetType(column_itr) == VALUE_TYPE_VARBINARY))
						&& !tuple_schema->IsInlined(column_itr)) {
					if (!GetValue(column_itr).IsNull()) {
						bytes += (sizeof(int32_t)
								+ ValuePeeker::PeekObjectLength(
										GetValue(column_itr)));
					}
				}
			}
		}

		return bytes;
	}

	//===--------------------------------------------------------------------===//
	// Serialization utilities
	//===--------------------------------------------------------------------===//

	void SerializeTo(SerializeOutput &output);
	void SerializeToExport(ExportSerializeOutput &output, int col_offset,
			uint8_t *null_array);
	void SerializeWithHeaderTo(SerializeOutput &output);

	void DeserializeFrom(SerializeInput &input, Pool *pool);
	int64_t DeserializeWithHeaderFrom(SerializeInput &input);

	size_t HashCode(size_t seed) const;
	size_t HashCode() const;

	/// Get a string representation of this tuple
	friend std::ostream& operator<< (std::ostream& os, const Tuple& tuple);
	std::string ToString(const std::string& table_name) const;
	std::string ToStringNoHeader() const;

protected:

	//===--------------------------------------------------------------------===//
	// Mask utilities
	//===--------------------------------------------------------------------===//

	/// Treat the first "Value" as a boolean flag
	inline void SetDeletedTrue() {
		*(reinterpret_cast<char*>(tuple_data)) |=
				static_cast<char>(DELETED_MASK);
	}

	inline void SetDeletedFalse() {
		*(reinterpret_cast<char*>(tuple_data)) &=
				static_cast<char>(~DELETED_MASK);
	}

	inline void SetDirtyTrue() {
		*(reinterpret_cast<char*>(tuple_data)) |= static_cast<char>(DIRTY_MASK);
	}

	inline void SetDirtyFalse() {
		*(reinterpret_cast<char*>(tuple_data)) &=
				static_cast<char>(~DIRTY_MASK);
	}

private:

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// The types of the columns in the tuple
	std::shared_ptr<catalog::TupleSchema> tuple_schema;

	/// The tuple data, padded at the front by the TUPLE_HEADER
	char *tuple_data;

	inline char* GetDataPtr(const int column_id) {
		assert(tuple_schema);
		assert(tuple_data);
		return &tuple_data[tuple_schema->GetOffset(column_id)
						   + TUPLE_HEADER_SIZE];
	}

	inline const char* GetDataPtr(const int column_id) const {
		assert(tuple_schema);
		assert(tuple_data);
		return &tuple_data[tuple_schema->GetOffset(column_id)
						   + TUPLE_HEADER_SIZE];
	}

};

//===--------------------------------------------------------------------===//
// Implementation
//===--------------------------------------------------------------------===//

/// Setup the tuple given the specified data location and schema
inline Tuple::Tuple(char *data, std::shared_ptr<catalog::TupleSchema> schema) {
	assert(data);
	assert(schema);

	tuple_data = data;
	tuple_schema = schema;
}

inline Tuple& Tuple::operator=(const Tuple &rhs) {
	tuple_schema = rhs.tuple_schema;
	tuple_data = rhs.tuple_data;
	return *this;
}

/// Set scalars by value and uninlined columns by reference into this tuple.
inline void Tuple::SetValue(const int column_id, Value value) {
	assert(tuple_schema);
	assert(tuple_data);

	const ValueType type = tuple_schema->GetType(column_id);
	value = value.CastAs(type);
	const bool is_inlined = tuple_schema->IsInlined(column_id);
	char *dataPtr = GetDataPtr(column_id);
	int32_t column_length = tuple_schema->GetLength(column_id);
	if(is_inlined == false)
		column_length = tuple_schema->GetVariableLength(column_id);
	value.Serialize(dataPtr, is_inlined, column_length);
}

/// Set all columns by value into this tuple.
inline void Tuple::SetValueAllocateForObjectCopies(const int column_id,
		Value value, Pool *dataPool) {
	assert(tuple_schema);
	assert(tuple_data);

	const ValueType type = tuple_schema->GetType(column_id);
	value = value.CastAs(type);
	const bool is_inlined = tuple_schema->IsInlined(column_id);
	char *dataPtr = GetDataPtr(column_id);
	int32_t column_length = tuple_schema->GetLength(column_id);
	if(is_inlined == false)
		column_length = tuple_schema->GetVariableLength(column_id);
	value.SerializeWithAllocation(dataPtr, is_inlined, column_length, dataPool);
}

/// For an insert, the copy should do an allocation for all uninlinable columns
inline void Tuple::CopyForInsert(const Tuple &source, Pool *pool) {
	assert(tuple_schema);
	assert(source.tuple_schema);
	assert(source.tuple_data);
	assert(tuple_data);

	const bool is_inlined = tuple_schema->IsInlined();
	std::shared_ptr<catalog::TupleSchema> source_schema = source.tuple_schema;
	const bool o_is_inlined = source_schema->IsInlined();
	const uint16_t uninlineable_column_count =
			tuple_schema->GetUninlinedColumnCount();

#ifndef NDEBUG
	if (!CompatibleForCopy(source)) {
		std::ostringstream message;
		message << "src  tuple  : " << source << std::endl;
		message << "src schema  : " << source.tuple_schema << std::endl;
		message << "dest schema : " << tuple_schema << std::endl;

		throw Exception(message.str());
	}
#endif

	if (is_inlined == o_is_inlined) {

		/**
		 * The source and target tuples have the same policy WRT to inlining.
		 * A memcpy can be used to speed the process up for all columns
		 * that are not uninlineable strings.
		 **/
		if (uninlineable_column_count > 0) {
			/// copy the data AND the header
			::memcpy(tuple_data, source.tuple_data,
					tuple_schema->GetLength() + TUPLE_HEADER_SIZE);

			/// Copy each uninlined column doing an allocation for copies.
			for (uint16_t column_itr = 0; column_itr < uninlineable_column_count; column_itr++) {
				const uint16_t unlineable_column_id =
						tuple_schema->GetUninlinedColumnIndex(column_itr);

				SetValueAllocateForObjectCopies(unlineable_column_id,
						source.GetValue(unlineable_column_id), pool);
			}

			tuple_data[0] = source.tuple_data[0];
		}
		else {
			/// copy the data AND the header
			::memcpy(tuple_data, source.tuple_data,
					tuple_schema->GetLength() + TUPLE_HEADER_SIZE);
		}
	} else {
		/// Can't copy the string ptr from the other tuple
		/// if the string is inlined into the tuple
		assert(!(!is_inlined && o_is_inlined));

		const uint16_t ColumnCount = tuple_schema->GetColumnCount();
		for (uint16_t column_itr = 0; column_itr < ColumnCount; column_itr++) {
			SetValueAllocateForObjectCopies(column_itr, source.GetValue(column_itr), pool);
		}

		tuple_data[0] = source.tuple_data[0];
	}
}

/// For an update, the copy should only do an allocation for a string
/// if the source and destination pointers are different.
inline void Tuple::CopyForUpdate(const Tuple &source, Pool *pool) {
	assert(tuple_schema);
	assert(tuple_schema == source.tuple_schema);

	const int column_count = tuple_schema->GetColumnCount();
	const uint16_t uninlineable_column_count = tuple_schema->GetUninlinedColumnCount();

	/**
	 * The source and target tuple have the same policy WRT to inlining.
	 * as they use the same schema.
	 */
	if (uninlineable_column_count > 0) {
		uint16_t uninlineable_column_itr = 0;
		uint16_t uninlineable_column_id = tuple_schema->GetUninlinedColumnIndex(0);

		/**
		 * Copy each column doing an allocation for string copies.
		 * Compare the source and target pointer to see if it
		 * is changed in this update. If it is changed then free the
		 * old string and copy/allocate the new one from the source.
		 */
		for (uint16_t column_itr = 0; column_itr < column_count; column_itr++) {
			if (column_itr == uninlineable_column_id) {
				const char *m_ptr = *reinterpret_cast<char* const *>(GetDataPtr(column_itr));
				const char *o_ptr = *reinterpret_cast<char* const *>(source.GetDataPtr(column_itr));

				if (m_ptr != o_ptr) {
					/// Make a copy of the input string. Don't need to delete the old string because
					/// that will be done by the UndoAction for the update.
					SetValueAllocateForObjectCopies(column_itr, source.GetValue(column_itr), pool);
				}

				uninlineable_column_itr++;
				if (uninlineable_column_itr < uninlineable_column_count) {
					uninlineable_column_id =
							tuple_schema->GetUninlinedColumnIndex(uninlineable_column_itr);
				}
				else {
					uninlineable_column_id = 0;
				}
			}
			else {
				SetValueAllocateForObjectCopies(column_itr, source.GetValue(column_itr), pool);
			}
		}

		tuple_data[0] = source.tuple_data[0];
	}
	else {
		/// copy the data AND the header
		::memcpy(tuple_data, source.tuple_data, tuple_schema->GetLength() + TUPLE_HEADER_SIZE);
	}
}

inline void Tuple::Copy(const Tuple &source) {
	assert(tuple_schema);
	assert(source.tuple_schema);
	assert(source.tuple_data);
	assert(tuple_data);

	const uint16_t column_count = tuple_schema->GetColumnCount();
	const bool is_inlined = tuple_schema->IsInlined();
	std::shared_ptr<catalog::TupleSchema> source_schema = source.tuple_schema;
	const bool o_is_inlined = source_schema->IsInlined();

#ifndef NDEBUG
	if (!CompatibleForCopy(source)) {
		std::ostringstream message;
		message << "src  tuple: " << source << std::endl;
		message << "src schema: " << source.tuple_schema << std::endl;
		message << "dest schema: " << tuple_schema << std::endl;
		throw Exception(message.str());
	}
#endif

	if (is_inlined == o_is_inlined) {
		/// copy the data AND the header
		::memcpy(tuple_data, source.tuple_data,
				tuple_schema->GetLength() + TUPLE_HEADER_SIZE);
	}
	else {
		/// Can't copy the string ptr from the other tuple if the
		/// string is inlined into the tuple
		assert(!(!is_inlined && o_is_inlined));

		for (uint16_t column_itr = 0; column_itr < column_count; column_itr++) {
			SetValue(column_itr, source.GetValue(column_itr));
		}

		tuple_data[0] = source.tuple_data[0];
	}
}

inline void Tuple::DeserializeFrom(SerializeInput &input, Pool *dataPool) {
	assert(tuple_schema);
	assert(tuple_data);

	input.ReadInt();
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		const ValueType type = tuple_schema->GetType(column_itr);

		/**
		 * DeserializeFrom is only called when we serialize/deserialize tables.
		 * The serialization format for Strings/Objects in a serialized table
		 * happens to have the same in memory representation as the Strings/Objects
		 * in a Tuple. The goal here is to wrap the serialized representation of
		 * the value in an Value and then serialize that into the tuple from the
		 * Value. This makes it possible to push more value specific functionality
		 * out of Tuple. The memory allocation will be performed when serializing
		 * to tuple storage.
		 */
		const bool is_inlined = tuple_schema->IsInlined(column_itr);
		char *data_ptr = GetDataPtr(column_itr);
		const int32_t column_length = tuple_schema->GetLength(column_itr);

		Value::DeserializeFrom(input, type, data_ptr, is_inlined, column_length, dataPool);
	}
}

inline int64_t Tuple::DeserializeWithHeaderFrom(SerializeInput &input) {

	int64_t total_bytes_deserialized = 0;

	assert(tuple_schema);
	assert(tuple_data);

	input.ReadInt();  // Read in the tuple size, discard
	total_bytes_deserialized += sizeof(int);

	memcpy(tuple_data, input.GetRawPointer(TUPLE_HEADER_SIZE), TUPLE_HEADER_SIZE);
	total_bytes_deserialized += TUPLE_HEADER_SIZE;
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		const ValueType type = tuple_schema->GetType(column_itr);

		const bool is_inlined = tuple_schema->IsInlined(column_itr);
		char *data_ptr = GetDataPtr(column_itr);
		const int32_t column_length = tuple_schema->GetLength(column_itr);
		total_bytes_deserialized +=
				Value::DeserializeFrom(input, type, data_ptr, is_inlined, column_length, NULL);
	}

	return total_bytes_deserialized;
}

inline void Tuple::SerializeWithHeaderTo(SerializeOutput &output) {
	assert(tuple_schema);
	assert(tuple_data);

	size_t start = output.Position();
	output.WriteInt(0);  // reserve first 4 bytes for the total tuple size

	output.WriteBytes(tuple_data, TUPLE_HEADER_SIZE);
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		Value value = GetValue(column_itr);
		value.SerializeTo(output);
	}

	int32_t serialized_size = static_cast<int32_t>(output.Position() - start - sizeof(int32_t));

	// write out the length of the tuple at start
	output.WriteIntAt(start, serialized_size);
}

inline void Tuple::SerializeTo(SerializeOutput &output) {
	size_t start = output.ReserveBytes(4);
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		Value value = GetValue(column_itr);
		value.SerializeTo(output);
	}

	output.WriteIntAt(start, static_cast<int32_t>(output.Position() - start - sizeof(int32_t)));
}

inline void Tuple::SerializeToExport(ExportSerializeOutput &output, int colOffset, uint8_t *null_array) {
	const int column_count = GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		// NULL doesn't produce any bytes for the Value
	    // Handle it here to consolidate manipulation of the nullarray.
		if (IsNull(column_itr)) {
			// turn on relevant bit in nullArray
			int byte = (colOffset + column_itr) >> 3;
			int bit = (colOffset + column_itr) % 8;
			int mask = 0x80 >> bit;
			null_array[byte] = (uint8_t) (null_array[byte] | mask);
			continue;
		}

		GetValue(column_itr).SerializeToExport(output);
	}
}

inline bool Tuple::operator==(const Tuple &other) const {
	if (tuple_schema != other.tuple_schema) {
		return false;
	}

	return EqualsNoSchemaCheck(other);
}

inline bool Tuple::operator!=(const Tuple &other) const {
	return !(*this == other);
}

inline bool Tuple::EqualsNoSchemaCheck(const Tuple &other) const {
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		const Value lhs = GetValue(column_itr);
		const Value rhs = other.GetValue(column_itr);
		if (lhs.OpNotEquals(rhs).IsTrue()) {
			return false;
		}
	}

	return true;
}

inline void Tuple::SetAllNulls() {
	assert(tuple_schema);
	assert(tuple_data);
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		Value value = Value::GetNullValue(tuple_schema->GetType(column_itr));
		SetValue(column_itr, value);
	}
}

inline int Tuple::Compare(const Tuple &other) const {
	int diff;
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		const Value lhs = GetValue(column_itr);
		const Value rhs = other.GetValue(column_itr);
		diff = lhs.Compare(rhs);

		if (diff) {
			return diff;
		}
	}

	return 0;
}

inline size_t Tuple::HashCode(size_t seed) const {
	const int column_count = tuple_schema->GetColumnCount();

	for (int column_itr = 0; column_itr < column_count; column_itr++) {
		const Value value = GetValue(column_itr);
		value.HashCombine(seed);
	}

	return seed;
}

inline size_t Tuple::HashCode() const {
	size_t seed = 0;
	return HashCode(seed);
}

/// Release to the heap any memory allocated for any uninlined columns.
inline void Tuple::FreeColumns() {
	const uint16_t unlinlined_column_count = tuple_schema->GetUninlinedColumnCount();

	for (int column_itr = 0; column_itr < unlinlined_column_count; column_itr++) {
		GetValue(tuple_schema->GetUninlinedColumnIndex(column_itr)).Free();
	}
}

/// Hasher
struct TupleHasher: std::unary_function<Tuple, std::size_t> {
	/// Generate a 64-bit number for the key value
	inline size_t operator()(Tuple tuple) const {
		return tuple.HashCode();
	}
};

/// Equality operator
class TupleEqualityChecker {
public:
	inline bool operator()(const Tuple lhs, const Tuple rhs) const {
		return lhs.EqualsNoSchemaCheck(rhs);
	}
};

} // End storage namespace
} // End nstore namespace

