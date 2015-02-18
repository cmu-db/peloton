/*-------------------------------------------------------------------------
 *
 * types.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/types.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <string>

namespace nstore {

/*
 * Value Types
 */

enum ValueType {
	VALUE_TYPE_INVALID = 0, 	// invalid type

	VALUE_TYPE_INTEGER = 1,		// 4 bytes integer
	VALUE_TYPE_DOUBLE = 2, 		// 8 bytes floating point
	VALUE_TYPE_VARCHAR = 3  	// variable length string

};

std::string GetTypeName(ValueType type);

bool IsNumeric(ValueType type);

} // End nstore namespace
