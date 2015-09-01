//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// numeric_functions.h
//
// Identification: src/backend/expression/numeric_functions.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"

namespace peloton {

/** implement the SQL ABS (absolute value) function for all numeric types */
template<> inline Value Value::CallUnary<FUNC_ABS>() const {
    if (IsNull()) {
        return *this;
    }
    const ValueType type = GetValueType();
    Value retval(type);
    switch(type) {
    /*abs() in C++ returns int (32-bits) if inputhis int, and long (64-bit) if input is long.
      VoltDB INTEGER.Is 32-bit, BIGINT is 64-bit, so for TINYINT (8-bit) and SMALLINT (16-bit),
      we need to cast.*/
    case VALUE_TYPE_TINYINT:
        retval.GetTinyInt() = static_cast<int8_t>(std::abs(GetTinyInt())); break;
    case VALUE_TYPE_SMALLINT:
        retval.GetSmallInt() = static_cast<int16_t>(std::abs(GetSmallInt())); break;
    case VALUE_TYPE_INTEGER:
        retval.GetInteger() = std::abs(GetInteger()); break;
    case VALUE_TYPE_BIGINT:
        retval.GetBigInt() = std::abs(GetBigInt()); break;
    case VALUE_TYPE_DOUBLE:
        retval.GetDouble() = std::abs(GetDouble()); break;
    case VALUE_TYPE_DECIMAL: {
        retval.GetDecimal() = GetDecimal();
        retval.GetDecimal().Abs_(); // updates in place!
    }
    break;
    case VALUE_TYPE_TIMESTAMP:
    default:
        ThrowCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}

/** implement the SQL FLOOR function for all numeric values */
template<> inline Value Value::CallUnary<FUNC_FLOOR>() const {
    if (IsNull()) {
        return *this;
    }
    const ValueType type = GetValueType();
    Value retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*floor() in C++ returns double (64-bits) if inputhis double, float (32-bit) if input is float,
      and long double (128-bit) if inputhis long double (128-bit).*/
    case VALUE_TYPE_DOUBLE:
        retval.GetDouble() = std::floor(GetDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        TTInt fractional(scaledValue);
        fractional %= Value::kMaxScaleFactor;
        if (fractional == 0) {
            return *this;
        }

        TTInt whole(scaledValue);
        whole /= Value::kMaxScaleFactor;
        if (scaledValue.IsSign()) {
            //whole has the sign at this point.
            whole--;
        }
        whole *= Value::kMaxScaleFactor;
        retval.GetDecimal() = whole;
    }
    break;
    default:
        ThrowCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}


/** implement the SQL CEIL function for all numeric values */
template<> inline Value Value::CallUnary<FUNC_CEILING>() const {
    if (IsNull()) {
        return *this;
    }
    const ValueType type = GetValueType();
    Value retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*ceil() in C++ returns double (64-bits) if inputhis double, float (32-bit) if input is float,
      and long double (128-bit) if inputhis long double (128-bit). VoltDB INTEGER is 32-bit, BIGINT is 64-bit,
      so for TINYINT (8-bit) and SMALLINT (16-bit), we need to cast.*/

    case VALUE_TYPE_DOUBLE:
        retval.GetDouble() = std::ceil(GetDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = GetDecimal();
        TTInt fractional(scaledValue);
        fractional %= Value::kMaxScaleFactor;
        if (fractional == 0) {
            return *this;
        }

        TTInt whole(scaledValue);
        whole /= Value::kMaxScaleFactor;
        if (!scaledValue.IsSign()) {
            whole++;
        }
        whole *= Value::kMaxScaleFactor;
        retval.GetDecimal() = whole;
    }
    break;
    default:
        ThrowCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}



/** implement the SQL SQRT function for all numeric values */
template<> inline Value Value::CallUnary<FUNC_SQRT>() const {
    if (IsNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    /*sqrt() in C++ returns double (64-bits) if inputhis double, float (32-bit) if input is float,
      and long double (128-bit) if inputhis long double (128-bit).*/
    double inputValue = CastAsDoubleAndGetValue();
    double resultDouble = std::sqrt(inputValue);
    ThrowDataExceptionIfInfiniteOrNaN(resultDouble, "function SQRT");
    retval.GetDouble() = resultDouble;
    return retval;
}


/** implement the SQL EXP function for all numeric values */
template<> inline Value Value::CallUnary<FUNC_EXP>() const {
    if (IsNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    //exp() in C++ returns double (64-bits) if inputhis double, float (32-bit) if input is float,
    //and long double (128-bit) if inputhis long double (128-bit).
    double exponentValue = CastAsDoubleAndGetValue();
    double resultDouble = std::exp(exponentValue);
    ThrowDataExceptionIfInfiniteOrNaN(resultDouble, "function EXP");
    retval.GetDouble() = resultDouble;
    return retval;
}


/** implement the SQL LOG/LN function for all numeric values */
template<> inline Value Value::CallUnary<FUNC_LN>() const {
    if (IsNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    double inputValue = CastAsDoubleAndGetValue();
    double resultDouble = std::log(inputValue);
    ThrowDataExceptionIfInfiniteOrNaN(resultDouble, "function LN");
    retval.GetDouble() = resultDouble;
    return retval;
}


/** implement the SQL POWER function for all numeric values */
template<> inline Value Value::Call<FUNC_POWER>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    Value retval(VALUE_TYPE_DOUBLE);
    const Value& base = arguments[0];
    const Value& exponent = arguments[1];

    if (base.IsNull()) {
        return base;
    }
    if (exponent.IsNull())
    {
      return exponent;
    }
    double baseValue = base.CastAsDoubleAndGetValue();
    double exponentValue = exponent.CastAsDoubleAndGetValue();
    double resultDouble = std::pow(baseValue, exponentValue);
    ThrowDataExceptionIfInfiniteOrNaN(resultDouble, "function POWER");
    retval.GetDouble() = resultDouble;
    return retval;
}

/**
 * FYI, http://stackoverflow.com/questions/7594508/modulo-operator-with-negative-values
 *
 * It looks like having any negative operand results in undefined behavior,
 * meaning different C++ compilers could Get different answers here.
 * In C++2003, the modulo operator (%).Is implementation defined.
 * In C++2011 the policy.Is slavish devotion to Fortran semantics. This makes sense because,
 * apparently, all the current hardware uses Fortran semantics anyway. So, even if it's implementation
 * defined it's likely to be implementation defined in the same way.
 *
 * FYI, Fortran semantics: https://gcc.gnu.org/onlinedocs/gfortran/MOD.html
 * It has the same semantics with C99 as: (a / b) * b + MOD(a,b)  == a
 */
template<> inline Value Value::Call<FUNC_MOD>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& base = arguments[0];
    const Value& divisor = arguments[1];

    const ValueType baseType = base.GetValueType();
    const ValueType divisorType = divisor.GetValueType();

    // planner should guard against any invalid number type
    if (!IsNumeric(baseType) || !IsNumeric(divisorType)) {
        throw Exception( "unsupported non-numeric type for SQL MOD function");
    }

    bool areAllIntegralType = IsIntegralType(baseType) && IsIntegralType(divisorType);

    if (! areAllIntegralType) {
        throw Exception("unsupported non-integral type for SQL MOD function");
    }
    if (base.IsNull() || divisor.IsNull()) {
        return GetNullValue(VALUE_TYPE_BIGINT);
    } else if (divisor.CastAsBigIntAndGetValue() == 0) {
        throw Exception("di.Ision by zero");
    }

    Value retval(divisorType);
    int64_t baseValue = base.CastAsBigIntAndGetValue();
    int64_t divisorValue = divisor.CastAsBigIntAndGetValue();

    int64_t result = std::abs(baseValue) % std::abs(divisorValue);
    if (baseValue < 0) {
        result *= -1;
    }

    return GetBigIntValue(result);
}

}  // End peloton namespace

