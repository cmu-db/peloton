/*-------------------------------------------------------------------------
 *
 * tuple_schema.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/abstract_tuple_schema.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <vector>

#include "common/types.h"

namespace nstore {
namespace catalog {

class AbstractTupleSchema	{

	//===--------------------------------------------------------------------===//
	// Column Information
	//===--------------------------------------------------------------------===//

	class ColumnInfo {
		ColumnInfo() = delete;
		ColumnInfo &operator=(const ColumnInfo &) = delete;

	public:

		ColumnInfo(ValueType column_type,
				uint32_t column_offset,
				uint32_t column_length,
				bool allow_null,
				bool is_inlined)
	: type(column_type), offset(column_offset),
	  variable_length(column_length), allow_null(allow_null),
	  is_inlined(is_inlined)
	{
			if(is_inlined)
				fixed_length = column_length;
			else
				fixed_length = sizeof(uintptr_t);
	}

		ValueType type;
		uint32_t offset;
		uint32_t fixed_length;
		uint32_t variable_length;
		char allow_null;
		bool is_inlined;
	};

public:
	AbstractTupleSchema() = delete;
	AbstractTupleSchema(const AbstractTupleSchema &) = delete;
	AbstractTupleSchema(AbstractTupleSchema &&) = delete;
	AbstractTupleSchema &operator=(const AbstractTupleSchema &) = delete;

	virtual ~AbstractTupleSchema(){
	};

	/// Construct schema
	AbstractTupleSchema(const std::vector<ValueType> column_types,
			const std::vector<uint32_t> column_lengths,
			const std::vector<bool> allow_null,
			const std::vector<bool> is_inlined);

	//===--------------------------------------------------------------------===//
	// Schema accessors
	//===--------------------------------------------------------------------===//

	inline uint32_t GetColumnOffset(const uint32_t column_id) const {
		return columns[column_id].offset;
	}

	inline ValueType GetColumnType(const uint32_t column_id) const {
		return columns[column_id].type;
	}

	inline uint32_t GetColumnFixedLength(const uint32_t column_id) const {
		return columns[column_id].fixed_length;
	}

	inline uint32_t GetColumnVariableLength(const uint32_t column_id) const {
		return columns[column_id].variable_length;
	}

	inline bool GetColumnIsInlined(const uint32_t column_id) const {
		return columns[column_id].is_inlined;
	}

	inline uint32_t GetTupleHeaderSize() const{
		return tuple_header_size;
	}

protected:
	uint32_t tuple_header_size;

	std::vector<ColumnInfo> columns;

};

} // End catalog namespace
} // End nstore namespace
