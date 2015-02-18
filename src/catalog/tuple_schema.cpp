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

#include "catalog/abstract_tuple_schema.h"

namespace nstore {
namespace catalog {

//===--------------------------------------------------------------------===//
// Tuple schema for hybrid tables
//===--------------------------------------------------------------------===//

class TupleSchema : public AbstractTupleSchema	{

	TupleSchema(const std::vector<ValueType> column_types,
			const std::vector<uint32_t> column_lengths,
			const std::vector<bool> allow_null,
			const std::vector<bool> is_inlined)
	: AbstractTupleSchema(column_types, column_lengths, allow_null, is_inlined)
	{

		/// Compute and set column offsets
		uint32_t column_offset = 0;
		uint32_t num_columns = column_types.size();

		for(uint32_t column_itr = 0 ; column_itr < num_columns ; column_itr++)	{
			columns[column_itr].offset = column_offset;
			column_offset  += GetColumnFixedLength(column_itr);
		}

		// Set tuple header size
		tuple_header_size = 8;
	}

};

} // End catalog namespace
} // End nstore namespace


