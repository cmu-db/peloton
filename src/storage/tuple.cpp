/*-------------------------------------------------------------------------
*
* abstract_tuple.cpp
* file description
*
* Copyright(c) 2015, CMU
*
* /n-store/src/storage/abstract_tuple.cpp
*
*-------------------------------------------------------------------------
*/

#include "tuple.h"

namespace nstore {
namespace storage {

void Tuple::SetField(const int column_id, Value value){
	const ValueType type = tuple_schema->Type(column_id);

	value = value.CastAs(type);

	const bool is_inlined = tuple_schema->IsInlined(column_id);
	char *field_ptr = GetFieldPtr(column_id);

	const int32_t column_length = tuple_schema->FixedLength(column_id);

	value.Serialize(field_ptr, is_inlined, column_length);
}

} // End storage namespace
} // End nstore namespace



