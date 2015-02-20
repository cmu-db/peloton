/*-------------------------------------------------------------------------
 *
 * types.cc
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/types.cc
 *
 *-------------------------------------------------------------------------
 */

#include "common/types.h"
#include "common/exception.h"

#include <cstring>

namespace nstore {

//===--------------------------------------------------------------------===//
// Type utilities
//===--------------------------------------------------------------------===//

std::string GetTypeName(ValueType type) {
	std::string ret;

	switch (type) {
	case (VALUE_TYPE_TINYINT):		return "tinyint";
	case (VALUE_TYPE_SMALLINT):		return "smallint";
	case (VALUE_TYPE_INTEGER):		return "integer";
	case (VALUE_TYPE_BIGINT):		return "bigint";
	case (VALUE_TYPE_DOUBLE):		return "double";
	case (VALUE_TYPE_VARCHAR):		return "varchar";
	case (VALUE_TYPE_VARBINARY):	return "varbinary";
	case (VALUE_TYPE_TIMESTAMP):	return "timestamp";
	case (VALUE_TYPE_DECIMAL):		return "decimal";
	case (VALUE_TYPE_INVALID):		return "INVALID";
	case (VALUE_TYPE_NULL):			return "NULL";
	default: {
		char buffer[32];
		snprintf(buffer, 32, "UNKNOWN[%d]", type);
		ret = buffer;
	}
	}
	return (ret);
}

std::string ValueToString(ValueType type){
	switch (type) {
	case VALUE_TYPE_INVALID:
		return "INVALID";
	case VALUE_TYPE_NULL:
		return "NULL";
	case VALUE_TYPE_TINYINT:
		return "TINYINT";
	case VALUE_TYPE_SMALLINT:
		return "SMALLINT";
	case VALUE_TYPE_INTEGER:
		return "INTEGER";
	case VALUE_TYPE_BIGINT:
		return "BIGINT";
	case VALUE_TYPE_DOUBLE:
		return "FLOAT";
	case VALUE_TYPE_VARCHAR:
		return "VARCHAR";
	case VALUE_TYPE_VARBINARY:
		return "VARBINARY";
	case VALUE_TYPE_TIMESTAMP:
		return "TIMESTAMP";
	case VALUE_TYPE_DECIMAL:
		return "DECIMAL";
	default:
		return "INVALID";
	}
}

ValueType StringToValue(std::string str ){
	if (str == "INVALID") {
		return VALUE_TYPE_INVALID;
	} else if (str == "NULL") {
		return VALUE_TYPE_NULL;
	} else if (str == "TINYINT") {
		return VALUE_TYPE_TINYINT;
	} else if (str == "SMALLINT") {
		return VALUE_TYPE_SMALLINT;
	} else if (str == "INTEGER") {
		return VALUE_TYPE_INTEGER;
	} else if (str == "BIGINT") {
		return VALUE_TYPE_BIGINT;
	} else if (str == "FLOAT") {
		return VALUE_TYPE_DOUBLE;
	} else if (str == "STRING") {
		return VALUE_TYPE_VARCHAR;
	} else if (str == "VARBINARY") {
		return VALUE_TYPE_VARBINARY;
	} else if (str == "TIMESTAMP") {
		return VALUE_TYPE_TIMESTAMP;
	} else if (str == "DECIMAL") {
		return VALUE_TYPE_DECIMAL;
	}
	else {
		throw ConversionException("No conversion from string :" + str);
	}
	return VALUE_TYPE_INVALID;
}


/** takes in 0-F, returns 0-15 */
int32_t hexCharToInt(char c) {
	c = static_cast<char>(toupper(c));
	if ((c < '0' || c > '9') && (c < 'A' || c > 'F')) {
		return -1;
	}
	int32_t retval;
	if (c >= 'A')
		retval = c - 'A' + 10;
	else
		retval = c - '0';

	assert(retval >=0 && retval < 16);
	return retval;
}

bool hexDecodeToBinary(unsigned char *bufferdst, const char *hexString) {
	assert (hexString);
	size_t len = strlen(hexString);
	if ((len % 2) != 0)
		return false;
	uint32_t i;
	for (i = 0; i < len / 2; i++) {
		int32_t high = hexCharToInt(hexString[i * 2]);
		int32_t low = hexCharToInt(hexString[i * 2 + 1]);
		if ((high == -1) || (low == -1))
			return false;
		int32_t result = high * 16 + low;

		assert (result >= 0 && result < 256);
		bufferdst[i] = static_cast<unsigned char>(result);
	}
	return true;
}


} // End nstore namespace
