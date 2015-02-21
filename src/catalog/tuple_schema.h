/*-------------------------------------------------------------------------
 *
 * tuple_schema.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "common/types.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Column Information
//===--------------------------------------------------------------------===//

class ColumnInfo {
	ColumnInfo() = delete;
	ColumnInfo &operator=(const ColumnInfo &) = delete;

public:

	// Configures all members except offset
	ColumnInfo(ValueType column_type, uint32_t column_length, bool allow_null,
			bool is_inlined)
: type(column_type), offset(0), allow_null(allow_null), is_inlined(is_inlined){

		if(is_inlined){
			fixed_length = column_length;
			variable_length = 0;
		}
		else{
			fixed_length = sizeof(uintptr_t);
			variable_length = column_length;
		}

	}

	/// Compare two column info objects
	bool operator== (const ColumnInfo &other) const {
		if (other.allow_null != allow_null || other.type != type ||
				other.is_inlined != is_inlined) {
			return false;
		}

		return true;
	}

	bool operator!= (const ColumnInfo &other) const {
		return !(*this == other);
	}

	// Configure offset as well
	ColumnInfo(ValueType column_type, uint32_t column_offset, uint32_t column_length,
			bool allow_null, bool is_inlined)
: type(column_type), offset(column_offset), allow_null(allow_null),
  is_inlined(is_inlined){

		if(is_inlined){
			fixed_length = column_length;
			variable_length = 0;
		}
		else{
			fixed_length = sizeof(uintptr_t);
			variable_length = column_length;
		}
	}

	/// Get a string representation for debugging
	std::string ToString() const;

	//===--------------------------------------------------------------------===//
	// Data members
	//===--------------------------------------------------------------------===//

	/// type of column
	ValueType type;

	/// offset of column in tuple
	uint32_t offset;

	/// if the column is not inlined, this is set to pointer size
	/// else, it is set to length of the fixed length column
	uint32_t fixed_length;

	/// if the column is inlined, this is set to 0
	/// else, it is set to length of the variable length column
	uint32_t variable_length;

	bool allow_null;
	bool is_inlined;
};

//===--------------------------------------------------------------------===//
// Tuple Schema
//===--------------------------------------------------------------------===//

class TupleSchema	{
	friend class ColumnInfo;

public:
	TupleSchema() = delete;
	TupleSchema(const TupleSchema &) = delete;
	TupleSchema(TupleSchema &&) = delete;
	TupleSchema &operator=(const TupleSchema &) = delete;

	//===--------------------------------------------------------------------===//
	// Static factory methods to construct schema objects
	//===--------------------------------------------------------------------===//

	/// Construct schema
	void CreateTupleSchema(const std::vector<ValueType> column_types,
			const std::vector<uint32_t> column_lengths,
			const std::vector<bool> allow_null,
			const std::vector<bool> is_inlined);

	/// Construct schema from vector of ColumnInfo
	TupleSchema(const std::vector<ColumnInfo> columns);

	/// Copy schema
	static TupleSchema *CopyTupleSchema(const TupleSchema *schema);

	/// Copy subset of columns in the given schema
	static TupleSchema *CopyTupleSchema(const TupleSchema *schema, const std::vector<uint32_t>& set);

	/// Append two schema objects
	static TupleSchema *AppendTupleSchema(const TupleSchema *first, const TupleSchema *second);

	/// Append subset of columns in the two given schemas
	static TupleSchema *AppendTupleSchema(const TupleSchema *first, const std::vector<uint32_t>& first_set,
			const TupleSchema *second, const std::vector<uint32_t>& second_set);

	/// Compare two schemas
	bool operator== (const TupleSchema &other) const;
	bool operator!= (const TupleSchema &other) const;

	//===--------------------------------------------------------------------===//
	// Schema accessors
	//===--------------------------------------------------------------------===//

	inline uint32_t Offset(const uint32_t column_id) const {
		return columns[column_id].offset;
	}

	inline ValueType Type(const uint32_t column_id) const {
		return columns[column_id].type;
	}

	inline uint32_t FixedLength(const uint32_t column_id) const {
		return columns[column_id].fixed_length;
	}

	inline uint32_t VariableLength(const uint32_t column_id) const {
		return columns[column_id].variable_length;
	}

	//// Get the nullability of the column at a given index.
	inline bool AllowNull(const uint32_t column_id) const {
		return columns[column_id].allow_null;
	}


	inline bool IsInlined(const uint32_t column_id) const {
		return columns[column_id].is_inlined;
	}

	/// Return the number of columns in the schema for the tuple.
	inline uint16_t ColumnCount() const {
		return columns.size();
	}

	/// Return the number of bytes used by one tuple.
	inline uint32_t TupleLength() const {
		return length;
	}

	/// Returns a flag indicating whether all columns are inlined
	bool IsInlined() const {
		return is_inlined;
	}

	uint16_t UninlinedObjectColumnCount() const {
		return uninlined_column_count;
	}

	/// Get a string representation of this schema for debugging
	std::string ToString() const;

	//===--------------------------------------------------------------------===//
	// Column info accessors
	//===--------------------------------------------------------------------===//

	const ColumnInfo GetColumnInfo(const uint32_t column_id) const {
		return columns[column_id];
	}

private:
	uint32_t length;

	std::vector<ColumnInfo> columns;

	/// are all columns inlined
	bool is_inlined;

	uint32_t uninlined_column_count;
};

} // End catalog namespace
} // End nstore namespace
