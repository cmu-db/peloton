/*-------------------------------------------------------------------------
 *
 * abstract_tuple_schema.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/catalog/abstract_tuple_schema.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "catalog/abstract_tuple_schema.h"

namespace nstore {
namespace catalog {

AbstractTupleSchema::AbstractTupleSchema(const std::vector<ValueType> column_types,
		const std::vector<uint32_t> column_lengths,
		const std::vector<bool> allow_null,
		const std::vector<bool> is_inlined)
{
	uint32_t num_columns = column_types.size();
	uint32_t column_offset = 0; 	// Set column_offset later in TupleSchema

	for(uint32_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{

		ColumnInfo column_info(column_types[column_itr],
				column_lengths[column_itr],
				column_offset,
				allow_null[column_itr],
				is_inlined[column_itr]);

		columns.push_back(column_info);
	}

	tuple_header_size = 0;
}

} // End catalog namespace
} // End nstore namespace
