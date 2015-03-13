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

#include <algorithm>
#include <sstream>
#include "schema.h"

namespace nstore {
namespace catalog {

/// Helper function for creating TupleSchema
void Schema::CreateTupleSchema(const std::vector<ValueType> column_types,
		const std::vector<uint32_t> column_lengths,
		const std::vector<bool> allow_null,
		const std::vector<bool> _is_inlined) {
	bool tuple_is_inlined = true;
	uint32_t num_columns = column_types.size();
	uint32_t column_offset = 0;

	for(uint32_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{

		ColumnInfo column_info(column_types[column_itr],
				column_offset,
				column_lengths[column_itr],
				allow_null[column_itr],
				_is_inlined[column_itr]);

		column_offset += column_info.fixed_length;

		columns.push_back(column_info);

		if(_is_inlined[column_itr] == false){
			tuple_is_inlined = false;
			uninlined_columns.push_back(column_itr);
		}
	}

	length = column_offset;
	is_inlined = tuple_is_inlined;

	column_count = columns.size();
	uninlined_column_count = uninlined_columns.size();
}

/// Construct schema from vector of ColumnInfo
Schema::Schema(const std::vector<ColumnInfo> columns){
	uint32_t column_count = columns.size();

	std::vector<ValueType> column_types;
	std::vector<uint32_t> column_lengths;
	std::vector<bool> allow_null;
	std::vector<bool> is_inlined;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++) {
		column_types.push_back(columns[column_itr].type);

		if(columns[column_itr].is_inlined)
			column_lengths.push_back(columns[column_itr].fixed_length);
		else
			column_lengths.push_back(columns[column_itr].variable_length);

		allow_null.push_back(columns[column_itr].allow_null);
		is_inlined.push_back(columns[column_itr].is_inlined);
	}

	CreateTupleSchema(column_types, column_lengths, allow_null, is_inlined);
}

/// Copy schema
Schema* Schema::CopySchema(const Schema	*schema) {
	uint32_t column_count = schema->GetColumnCount();
	std::vector<uint32_t> set;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++)
		set.push_back(column_itr);

	return CopySchema(schema, set);
}

/// Copy subset of columns in the given schema
Schema* Schema::CopySchema(const Schema *schema,
		const std::vector<uint32_t>& set){
	uint32_t column_count = schema->GetColumnCount();
	std::vector<ColumnInfo> columns;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++) {
		// If column exists in set
		if(std::find(set.begin(), set.end(), column_itr) != set.end()) {
			columns.push_back(schema->columns[column_itr]);
		}
	}

	Schema *ret_schema = new Schema(columns);
	return ret_schema;
}

/// Append two schema objects
Schema* Schema::AppendSchema(const Schema *first, const Schema *second){
	uint32_t column_count1, column_count2;
	std::vector<uint32_t> set1, set2;

	column_count1 = first->GetColumnCount();
	column_count2 = second->GetColumnCount();

	for (uint32_t column_itr = 0; column_itr < column_count1; column_itr++)
		set1.push_back(column_itr);
	for (uint32_t column_itr = 0; column_itr < column_count2; column_itr++)
		set2.push_back(column_itr);

	return AppendSchema(first, set1, second, set2);
}

/// Append subset of columns in the two given schemas
Schema* Schema::AppendSchema(const Schema *first,
		const std::vector<uint32_t>& first_set,
		const Schema *second, const std::vector<uint32_t>& second_set) {
	uint32_t column_count1, column_count2;
	std::vector<ColumnInfo> columns;

	column_count1 = first->GetColumnCount();
	column_count2 = second->GetColumnCount();

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

	Schema *ret_schema = new Schema(columns);

	return ret_schema;
}

/// Get a string representation
std::ostream& operator<< (std::ostream& os, const ColumnInfo& column_info){
	os << " type = " << GetValueTypeName(column_info.type) << "," <<
			" offset = " << column_info.offset << "," <<
			" fixed length = " << column_info.fixed_length << "," <<
			" variable length = " << column_info.variable_length << "," <<
			" nullable = " << column_info.allow_null <<
			" inlined = " << column_info.is_inlined << std::endl;

	return os;
}

/// Get a string representation of this schema for debugging
std::ostream& operator<< (std::ostream& os, const Schema& schema){
	os << "\tSchema :: " <<
			" column_count = " << schema.column_count <<
			" is_inlined = " << schema.is_inlined << "," <<
			" length = " << schema.length << "," <<
			" uninlined_column_count = " << schema.uninlined_column_count << std::endl;

	for (id_t column_itr = 0; column_itr < schema.column_count; column_itr++) {
		os << "\t Column " << column_itr << " :: " << schema.columns[column_itr];
	}

	return os;
}

/// Compare two schemas
bool Schema::operator== (const Schema &other) const {

	if (other.GetColumnCount() != GetColumnCount() ||
			other.GetUninlinedColumnCount() != GetUninlinedColumnCount() ||
			other.IsInlined() != IsInlined()) {
		return false;
	}

	for (id_t column_itr = 0; column_itr < other.GetColumnCount(); column_itr++) {
		const ColumnInfo& column_info = other.GetColumnInfo(column_itr);
		const ColumnInfo& other_column_info = GetColumnInfo(column_itr);

		if (column_info != other_column_info) {
			return false;
		}
	}

	return true;
}

bool Schema::operator!= (const Schema &other) const {
	return !(*this == other);
}

} // End catalog namespace
} // End nstore namespace
