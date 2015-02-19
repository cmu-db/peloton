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

//===--------------------------------------------------------------------===//
// NULL-related Constants
//===--------------------------------------------------------------------===//

/// NULL
#define INT8_NULL         INT8_MIN
#define INT16_NULL        INT16_MIN
#define INT32_NULL        INT32_MIN
#define INT64_NULL        INT64_MIN

/// Minimum value user can represent that is not NULL
#define NSTORE_INT8_MIN 		INT8_NULL  + 1
#define NSTORE_INT16_MIN 		INT16_NULL + 1
#define NSTORE_INT32_MIN 		INT32_NULL + 1
#define NSTORE_INT64_MIN 		INT64_NULL + 1
#define DECIMAL_MIN 	-9999999
#define DECIMAL_MAX 	9999999

/// Float/Double less than these values are NULL
#define FLOAT_NULL 		-3.4e+38f
#define DOUBLE_NULL 	-1.7E+308

// Values to be substituted as NULL
#define FLOAT_MIN 		-3.40282347e+38f
#define DOUBLE_MIN 		-1.7976931348623157E+308

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
#define OBJECTLENGTH_NULL			 -1
#define VALUE_COMPARE_LESSTHAN 		 -1
#define VALUE_COMPARE_EQUAL 		  0
#define VALUE_COMPARE_GREATERTHAN 	  1


//===--------------------------------------------------------------------===//
// ENUM types
//===--------------------------------------------------------------------===//

enum ValueType {
	VALUE_TYPE_INVALID = 0, 	// invalid type
	VALUE_TYPE_NULL = 1, 		// NULL type

	VALUE_TYPE_TINYINT = 3, 	// 1 byte integer
	VALUE_TYPE_SMALLINT = 4,	// 2 bytes integer
	VALUE_TYPE_INTEGER = 5,		// 4 bytes integer
	VALUE_TYPE_BIGINT = 6, 		// 8 bytes integer
	VALUE_TYPE_DOUBLE = 8, 		// 8 bytes floating number
	VALUE_TYPE_VARCHAR = 9, 	// variable length chars
	VALUE_TYPE_TIMESTAMP = 11, 	// 8 bytes integer
	VALUE_TYPE_DECIMAL = 22, 	// decimal(p,s)
	VALUE_TYPE_BOOLEAN = 23,
	VALUE_TYPE_ADDRESS = 24,
	VALUE_TYPE_VARBINARY = 25, 	// variable length bytes
};

std::string GetTypeName(ValueType type);

bool IsNumeric(ValueType type);

int32_t hexCharToInt(char c);

bool hexDecodeToBinary(unsigned char *bufferdst, const char *hexString);

std::string ValueToString(ValueType type);

ValueType StringToValue(std::string str );

} // End nstore namespace
