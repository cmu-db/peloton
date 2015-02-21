/*-------------------------------------------------------------------------
 *
 * tuple.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/storage/abstract_tuple.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/tuple.h"

#include <cstdlib>
#include <sstream>
#include <cassert>

#include "storage/tuple.h"
#include "common/exception.h"

namespace nstore {
namespace storage {

std::ostream& operator<< (std::ostream& os, const Tuple& tuple){
	if (tuple.IsAlive() == false) {
		os << " <DELETED>";
	} else {
		int column_count = tuple.GetColumnCount();
		for (int column_itr = 0; column_itr < column_count; column_itr++) {
			os << "(";
			if (tuple.IsNull(column_itr)) {
				os << "<NULL>";
			}
			else {
				os << tuple.GetValue(column_itr).ToString();
			}
			os << ")";
		}
	}

	uint64_t address_num = (uint64_t) tuple.Location();
	os << " @" << address_num;

	os << std::endl;

	return os;
}

std::string Tuple::ToString(const std::string& table_name) const {
	assert(tuple_schema);
	assert(tuple_data);

	std::ostringstream buffer;

	if (table_name.empty()) {
		buffer << "Tuple(no table)";
	} else {
		buffer << "Tuple(" << table_name << ")";
	}

	buffer << (*this);

	std::string ret(buffer.str());
	return ret;
}

std::string Tuple::ToStringNoHeader() const {
	assert(tuple_schema);
	assert(tuple_data);

	std::ostringstream buffer;
	buffer << "Tuple(notable) ->";

	int column_count = GetColumnCount();
	for (int column_itr = 0; column_itr < column_count; column_itr++) {

		if (IsNull(column_itr)) {
			buffer << "<NULL>";
		}
		else {
			buffer << "(" << GetValue(column_itr).ToString() << ")";
		}
	}

	buffer << std::endl;
	std::string ret(buffer.str());
	return ret;
}

bool Tuple::CompatibleForCopy(const Tuple &source) {

	if (tuple_schema->GetColumnCount() != source.tuple_schema->GetColumnCount()) {
		//ERROR("Can not copy tuple: incompatible column count.");
		return false;
	}

	int column_count = GetColumnCount();
	for (int column_itr = 0; column_itr < column_count; column_itr++) {

		const ValueType type = tuple_schema->GetType(column_itr);
		const ValueType s_type = source.tuple_schema->GetType(column_itr);

		if (type != s_type) {
			//ERROR("Can not copy tuple: incompatible column types.");
			return false;
		}

		const bool is_inlined = tuple_schema->IsInlined();
		const bool s_is_inlined = source.tuple_schema->IsInlined();

		if (is_inlined && s_is_inlined) {
			const bool column_is_inlined = tuple_schema->IsInlined(column_itr);
			const bool s_column_is_inlined = source.tuple_schema->IsInlined(column_itr);

			if (column_is_inlined != s_column_is_inlined) {
				//ERROR("Can not copy tuple: incompatible column inlining.");
				return false;
			}
		}
		else {
			const int32_t m_column_length = tuple_schema->GetLength(column_itr);
			const int32_t s_column_length = source.tuple_schema->GetLength(column_itr);

			if (m_column_length < s_column_length) {
				//ERROR("Can not copy tuple: incompatible column lengths.");
				return false;
			}
		}
	}

	return true;
}

} // End storage namespace
} // End nstore namespace



