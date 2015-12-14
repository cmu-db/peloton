//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// value_Peeker.h
//
// Identification: src/backend/common/value_Peeker.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"
#include "backend/common/types.h"

#include <cassert>

namespace peloton {

/**
 * A class for Peeking into an Value and converting its data to a
 * native C++ type.
 *
 * It is necessary for some classes to have access to the actual value
 * in order to serialize, format for printing, or run tests.
 * Moving the functionality for accessing the private data into
 * these static methods allows Value to define ValuePeeker as its
 * only friend class. Anything that uses this class is a possible
 * candidate for having its functionality moved into Value to ensure
 * consistency.
 */
class ValuePeeker {
public:
    static inline double PeekDouble(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_DOUBLE);
        return value.GetDouble();
    }

    static inline int8_t PeekTinyInt(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_TINYINT);
        return value.GetTinyInt();
    }

    static inline int16_t PeekSmallInt(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_SMALLINT);
        return value.GetSmallInt();
    }

    static inline int32_t PeekInteger(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_INTEGER);
        return value.GetInteger();
    }

    static inline bool PeekBoolean(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_BOOLEAN);
        return value.GetBoolean();
    }

    // cast as int and Peek at value. this is used by index code that need a
    // real number from a tuple and the limit node code used to get the limit
    // from an expression.
    static inline int32_t PeekAsInteger(const Value& value) {
        return value.CastAsInteger().GetInteger();
    }

    static inline int64_t PeekBigInt(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_BIGINT);
        return value.GetBigInt();
    }

    static inline int64_t PeekTimestamp(const Value& value) {
        assert(value.GetValueType() == VALUE_TYPE_TIMESTAMP);
        return value.GetTimestamp();
    }

    static inline void* PeekObjectValue(const Value& value) {
        assert((value.GetValueType() == VALUE_TYPE_VARCHAR) ||
               (value.GetValueType() == VALUE_TYPE_VARBINARY));
        return value.GetObjectValue();
    }

    static inline void* PeekObjectValueWithoutNull(const Value& value) {
        assert((value.GetValueType() == VALUE_TYPE_VARCHAR) ||
               (value.GetValueType() == VALUE_TYPE_VARBINARY));
        return value.GetObjectValueWithoutNull();
    }

    static inline int32_t PeekObjectLengthWithoutNull(const Value& value) {
        assert((value.GetValueType() == VALUE_TYPE_VARCHAR) ||
               (value.GetValueType() == VALUE_TYPE_VARBINARY));
        return value.GetObjectLengthWithoutNull();
    }

    /**
     * This function is only used in 'nvalue_test.cpp', why test a function that
     * is not used in source code? Get rid of it? -xin
     */
    static std::string PeekStringCopyWithoutNull(const Value& value) {
        assert((value.GetValueType() == VALUE_TYPE_VARCHAR) ||
               (value.GetValueType() == VALUE_TYPE_VARBINARY));
        std::string result(reinterpret_cast<const char*>(value.GetObjectValueWithoutNull()),
                                                         value.GetObjectLengthWithoutNull());
        return result;
    }

    static inline ValueType PeekValueType(const Value& value) {
        return value.GetValueType();
    }

    static inline TTInt PeekDecimal(const Value& value) {
        return value.GetDecimal();
    }

    // exists for test.
    static inline std::string PeekDecimalString(const Value& value) {
        return value.CreateStringFromDecimal();
    }

    // cast as big int and Peek at value. this is used by
    // index code that need a real number from a tuple.
    static inline int64_t PeekAsBigInt(const Value& value) {
        if (value.IsNull()) {
            return INT64_NULL;
        }
        return value.CastAsBigIntAndGetValue();
    }

    static inline int64_t PeekAsRawInt64(const Value& value) {
        return value.CastAsBigIntAndGetValue();
    }

    /// Given an Value, return a pointer to its data bytes.  Also return
    /// The length of the data bytes via output parameter.
    ///
    /// Assumes that value is not null!!
    static inline const char* PeekPointerToDataBytes(const Value& value, int32_t *length) {
        ValueType vt = value.GetValueType();
        switch (vt) {
        case VALUE_TYPE_TINYINT:
        case VALUE_TYPE_SMALLINT:
        case VALUE_TYPE_INTEGER:
        case VALUE_TYPE_BIGINT:
        case VALUE_TYPE_TIMESTAMP:
        case VALUE_TYPE_DECIMAL:
        case VALUE_TYPE_BOOLEAN:
            *length = static_cast<int32_t>(Value::GetTupleStorageSize(vt));
            return value.m_data;

        default:
            assert(false);
            return NULL;
        }
    }
};

}  // End peloton namespace
