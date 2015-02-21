/*-------------------------------------------------------------------------
 *
 * tuple_schema.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/tuple_schema.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "tuple_schema.h"

#include <algorithm>
#include <sstream>

namespace nstore {
namespace catalog {

/// Helper function for creating TupleSchema
void TupleSchema::CreateTupleSchema(const std::vector<ValueType> column_types,
		const std::vector<uint32_t> column_lengths,
		const std::vector<bool> allow_null,
		const std::vector<bool> _is_inlined) {
	bool tuple_is_inlined = true;
	uint32_t uninlined_columns = 0;
	uint32_t num_columns = column_types.size();
	uint32_t column_offset = 0;

	for(uint32_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{

		ColumnInfo column_info(column_types[column_itr],
				column_offset,
				column_lengths[column_itr],
				allow_null[column_itr],
				_is_inlined[column_itr]);

		column_offset += column_info.fixed_length;

		if(_is_inlined[column_itr] == false){
			tuple_is_inlined = false;
			uninlined_columns++;
		}

		columns.push_back(column_info);
	}

	length = column_offset;
	is_inlined = tuple_is_inlined;
	uninlined_column_count = uninlined_columns;
}

/// Construct schema from vector of ColumnInfo
TupleSchema::TupleSchema(const std::vector<ColumnInfo> columns){
	uint32_t column_count = columns.size();

	std::vector<ValueType> column_types;
	std::vector<uint32_t> column_lengths;
	std::vector<bool> allow_null;
	std::vector<bool> _is_inlined;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++) {
		column_types.push_back(columns[column_itr].type);
		column_lengths.push_back(columns[column_itr].fixed_length);
		allow_null.push_back(columns[column_itr].allow_null);
		_is_inlined.push_back(columns[column_itr].is_inlined);
	}

	length = 0;
	is_inlined = false;
	uninlined_column_count = 0;

	CreateTupleSchema(column_types, column_lengths, allow_null, _is_inlined);
}

/// Copy schema
TupleSchema* TupleSchema::CopyTupleSchema(const TupleSchema	*schema) {
	uint32_t column_count = schema->ColumnCount();
	std::vector<uint32_t> set;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++)
		set.push_back(column_itr);

	return CopyTupleSchema(schema, set);
}

/// Copy subset of columns in the given schema
TupleSchema* TupleSchema::CopyTupleSchema(const TupleSchema *schema,
		const std::vector<uint32_t>& set){
	uint32_t column_count = schema->ColumnCount();
	std::vector<ColumnInfo> columns;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++) {
		// If column exists in set
		if(std::find(set.begin(), set.end(), column_itr) != set.end()) {
			columns.push_back(schema->columns[column_itr]);
		}
	}

	TupleSchema *ret_schema = new TupleSchema(columns);
	return ret_schema;
}

/// Append two schema objects
TupleSchema* TupleSchema::AppendTupleSchema(const TupleSchema *first, const TupleSchema *second){
	uint32_t column_count1, column_count2;
	std::vector<uint32_t> set1, set2;

	column_count1 = first->ColumnCount();
	column_count2 = second->ColumnCount();

	for (uint32_t column_itr = 0; column_itr < column_count1; column_itr++)
		set1.push_back(column_itr);
	for (uint32_t column_itr = 0; column_itr < column_count2; column_itr++)
		set2.push_back(column_itr);

	return AppendTupleSchema(first, set1, second, set2);
}

/// Append subset of columns in the two given schemas
TupleSchema* TupleSchema::AppendTupleSchema(const TupleSchema *first,
		const std::vector<uint32_t>& first_set,
		const TupleSchema *second, const std::vector<uint32_t>& second_set) {
	uint32_t column_count1, column_count2;
	std::vector<ColumnInfo> columns;

	column_count1 = first->ColumnCount();
	column_count2 = second->ColumnCount();

	for (uint32_t column_itr = 0; column_itr < column_count1; column_itr++) {
		// If column exists in first set
		if(std::find(first_set.begin(), first_set.end(), column_itr) != first_set.end()) {
			columns.push_back(first->columns[column_itr]);
		}
	}

	for (uint32_t column_itr = 0; column_itr < column_count2; column_itr++) {
		// If column exists in second set
		if(std::find(second_set.begin(), second_set.end(), column_itr) != second_set.end()) {
			columns.push_back(second->columns[column_itr]);
		}
	}

	TupleSchema *ret_schema = new TupleSchema(columns);

	return ret_schema;
}

/// Get a string representation
std::string ColumnInfo::ToString() const{
	std::ostringstream buffer;

	buffer << " type = " << GetTypeName(type) << "," <<
			" offset = " << offset << "," <<
			" length = " << fixed_length << "," <<
			" allow_null = " << allow_null <<
			" is_inlined = " << is_inlined << std::endl;

	std::string ret(buffer.str());
	return ret;
}

/// Get a string representation of this schema for debugging
std::string TupleSchema::ToString() const{
	std::ostringstream buffer;

	buffer << "Schema :: " <<
			" column_count = " << ColumnCount() <<
			" is_inlined = " << is_inlined << "," <<
			" length = " << length << "," <<
			" uninlined_column_count = " << uninlined_column_count << std::endl;

	for (uint32_t column_itr = 0; column_itr < ColumnCount(); column_itr++) {
		buffer << " Column " << column_itr << " :: " << columns[column_itr].ToString();
	}

	std::string ret(buffer.str());
	return ret;
}

/// Compare two schemas
bool TupleSchema::operator== (const TupleSchema &other) const {

	if (other.ColumnCount() != ColumnCount() ||
			other.UninlinedObjectColumnCount() != UninlinedObjectColumnCount() ||
			other.IsInlined() != IsInlined()) {
		return false;
	}

	for (int column_itr = 0; column_itr < other.ColumnCount(); column_itr++) {
		const ColumnInfo& column_info = other.GetColumnInfo(column_itr);
		const ColumnInfo& other_column_info = GetColumnInfo(column_itr);

		if (column_info != other_column_info) {
			return false;
		}
	}

	return true;
}

bool TupleSchema::operator!= (const TupleSchema &other) const {
	return !(*this == other);
}

} // End catalog namespace
} // End nstore namespace
