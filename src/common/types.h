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
#include <cstdint>
#include <climits>
#include <limits>

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
// Other Constants
//===--------------------------------------------------------------------===//

#define VARCHAR_LENGTH_SHORT 	16
#define VARCHAR_LENGTH_MID  	256
#define VARCHAR_LENGTH_LONG  	4096

//===--------------------------------------------------------------------===//
// ENUM types
//===--------------------------------------------------------------------===//

enum ValueType {
	VALUE_TYPE_INVALID = 0, 	// invalid value type
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

enum BackendType {
	BACKEND_TYPE_INVALID = 0, 		// invalid backend type

	BACKEND_TYPE_VM = 1, 			// on volatile memory
	BACKEND_TYPE_NVM = 2 			// on non-volatile memory
};

//===--------------------------------------------------------------------===//
// Type definitions.
//===--------------------------------------------------------------------===//

typedef uint64_t id_t;

static const id_t INVALID_ID = -1;

static const id_t INVALID_CATALOG_ID = 0;

static const id_t MAX_ID = std::numeric_limits<id_t>::max();


//===--------------------------------------------------------------------===//
// Type utilities
//===--------------------------------------------------------------------===//

std::string GetValueTypeName(ValueType type);

std::string GetBackendTypeName(BackendType type);

std::string ValueToString(ValueType type);

ValueType StringToValue(std::string str );

/// Works only for fixed-length types
std::size_t GetTypeSize(ValueType type);

bool IsNumeric(ValueType type);

bool HexDecodeToBinary(unsigned char *bufferdst, const char *hexString);


} // End nstore namespace
