/*-------------------------------------------------------------------------
 *
 * exception.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/common/exception.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "common/types.h"

#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <stdexcept>

namespace nstore {

//===--------------------------------------------------------------------===//
// Exception Types
//===--------------------------------------------------------------------===//

enum ExceptionType {
	EXCEPTION_TYPE_INVALID = 0, 					// invalid type

	EXCEPTION_TYPE_OUT_OF_RANGE = 1, 				// value out of range error
	EXCEPTION_TYPE_CONVERSION = 2, 					// conversion/casting error
	EXCEPTION_TYPE_UNKNOWN_TYPE = 3,				// unknown type
	EXCEPTION_TYPE_DECIMAL = 4,						// decimal related
	EXCEPTION_TYPE_MISMATCH_TYPE = 5,				// type mismatch
	EXCEPTION_TYPE_DIVIDE_BY_ZERO = 6,				// divide by 0
	EXCEPTION_TYPE_OBJECT_SIZE = 7,					// object size exceeded
	EXCEPTION_TYPE_INCOMPATIBLE_TYPE = 8			// incompatible for operation
};

class Exception {
public:

	Exception(std::string message) {
		throw std::runtime_error(message);
	}

	Exception(ExceptionType exception_type, std::string message){
		throw std::runtime_error(std::to_string(exception_type) + " " +	message);
	}

};

//===--------------------------------------------------------------------===//
// Exception derived classes
//===--------------------------------------------------------------------===//

class CastException : Exception {
	CastException() = delete;

public:
	CastException(const ValueType origType, const ValueType newType) :
		Exception(EXCEPTION_TYPE_CONVERSION,
				"Type " + ValueToString(origType) + "can't be cast as " + ValueToString(newType)){
	}

};

class ValueOutOfRangeException : Exception {
	ValueOutOfRangeException() = delete;

public:
	ValueOutOfRangeException(const int64_t value, const ValueType origType, const ValueType newType) :
		Exception(EXCEPTION_TYPE_CONVERSION,
				"Type " +ValueToString(origType) + "with value " + std::to_string((intmax_t)value) +
				" can't be cast as %s because the value is out of range for the destination type " +
				ValueToString(newType)){
	}

	ValueOutOfRangeException(const double value, const ValueType origType, const ValueType newType) :
		Exception(EXCEPTION_TYPE_CONVERSION,
				"Type " +ValueToString(origType) + "with value " + std::to_string(value) +
				" can't be cast as %s because the value is out of range for the destination type " +
				ValueToString(newType)){
	}
};


class ConversionException : Exception {
	ConversionException() = delete;

public:
	ConversionException(std::string msg) :
		Exception(EXCEPTION_TYPE_CONVERSION, msg){
	}
};

class UnknownTypeException : Exception {
	UnknownTypeException() = delete;

public:
	UnknownTypeException(int type, std::string msg) :
		Exception(EXCEPTION_TYPE_UNKNOWN_TYPE, "unknown type " + std::to_string(type) + msg){
	}
};

class DecimalException : Exception {
	DecimalException() = delete;

public:
	DecimalException(std::string msg) :
		Exception(EXCEPTION_TYPE_DECIMAL, msg){
	}
};

class TypeMismatchException : Exception {
	TypeMismatchException() = delete;

public:
	TypeMismatchException(std::string msg, const ValueType type_1, const ValueType type_2) :
		Exception(EXCEPTION_TYPE_MISMATCH_TYPE,
				"Type " +ValueToString(type_1) + " does not match with " + ValueToString(type_2) + msg){
	}
};

class NumericValueOutOfRangeException : Exception {
	NumericValueOutOfRangeException() = delete;

public:
	NumericValueOutOfRangeException(std::string msg) :
		Exception(EXCEPTION_TYPE_OUT_OF_RANGE, msg){
	}
};

class DivideByZeroException : Exception {
	DivideByZeroException() = delete;

public:
	DivideByZeroException(std::string msg) :
		Exception(EXCEPTION_TYPE_DIVIDE_BY_ZERO, msg){
	}
};

class ObjectSizeException : Exception {
	ObjectSizeException() = delete;

public:
	ObjectSizeException(std::string msg) :
		Exception(EXCEPTION_TYPE_OBJECT_SIZE, msg){
	}
};

class IncompatibleTypeException : Exception {
	IncompatibleTypeException() = delete;

public:
	IncompatibleTypeException(int type, std::string msg) :
		Exception(EXCEPTION_TYPE_INCOMPATIBLE_TYPE, "Incompatible type " + std::to_string(type) + msg){
	}
};



} // End nstore namespace

