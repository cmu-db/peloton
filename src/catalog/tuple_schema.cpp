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

/// Ensure sure all vectors are of same size.
TupleSchema::TupleSchema(const std::vector<ValueType> column_types,
		const std::vector<uint32_t> column_lengths,
		const std::vector<bool> allow_null,
		const std::vector<bool> _is_inlined){

	bool tuple_is_inlined = true;
	uint32_t uninlined_columns = 0;
	uint32_t num_columns = column_types.size();
	uint32_t column_offset = 0;

	for(uint32_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{

		ColumnInfo column_info(column_types[column_itr],
				column_lengths[column_itr],
				column_offset,
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

	std::vector<ValueType> column_types;
	std::vector<uint32_t> column_lengths;
	std::vector<bool> allow_null;
	std::vector<bool> is_inlined;

	for (uint32_t column_itr = 0; column_itr < column_count; column_itr++) {
		// If column is in the returned schema
		if(std::find(set.begin(), set.end(), column_itr) != set.end()) {
			column_types.push_back(schema->Type(column_itr));
			column_lengths.push_back(schema->FixedLength(column_itr));
			allow_null.push_back(schema->AllowNull(column_itr));
			is_inlined.push_back(schema->IsInlined(column_itr));
		}
	}

	TupleSchema *ret_schema = new TupleSchema(column_types, column_lengths,
			allow_null, is_inlined);

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

	column_count1 = first->ColumnCount();
	column_count2 = second->ColumnCount();

	std::vector<ValueType> column_types;
	std::vector<uint32_t> column_lengths;
	std::vector<bool> allow_null;
	std::vector<bool> is_inlined;

	for (uint32_t column_itr = 0; column_itr < column_count1; column_itr++) {
		// If column in first schema is in the returned schema
		if(std::find(first_set.begin(), first_set.end(), column_itr) != first_set.end()) {
			column_types.push_back(first->Type(column_itr));
			column_lengths.push_back(first->FixedLength(column_itr));
			allow_null.push_back(first->AllowNull(column_itr));
			is_inlined.push_back(first->IsInlined(column_itr));
		}
	}

	for (uint32_t column_itr = 0; column_itr < column_count2; column_itr++) {
		// If column in second schema is in the returned schema
		if(std::find(second_set.begin(), second_set.end(), column_itr) != second_set.end()) {
			column_types.push_back(second->Type(column_itr));
			column_lengths.push_back(second->FixedLength(column_itr));
			allow_null.push_back(second->AllowNull(column_itr));
			is_inlined.push_back(second->IsInlined(column_itr));
		}
	}

	TupleSchema *ret_schema = new TupleSchema(column_types, column_lengths,
			allow_null, is_inlined);

	return ret_schema;
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
		buffer << " Column " << column_itr << " :: " <<
				" type = " << GetTypeName(Type(column_itr)) << "," <<
				" length = " << FixedLength(column_itr) << "," <<
				" allow_null = " << AllowNull(column_itr) << "," <<
				" is_inlined = " << IsInlined(column_itr) << std::endl;
	}

	std::string ret(buffer.str());
	return ret;
}

/// Compare two schemas
bool TupleSchema::operator== (TupleSchema &other){

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

} // End catalog namespace
} // End nstore namespace
