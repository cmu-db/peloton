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
namespace expression {

/** implement the SQL ABS (absolute value) function for all numeric types */
template<> inline Value Value::callUnary<FUNC_ABS>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    Value retval(type);
    switch(type) {
    /*abs() in C++ returns int (32-bits) if input is int, and long (64-bit) if input is long.
      VoltDB INTEGER is 32-bit, BIGINT is 64-bit, so for TINYINT (8-bit) and SMALLINT (16-bit),
      we need to cast.*/
    case VALUE_TYPE_TINYINT:
        retval.getTinyInt() = static_cast<int8_t>(std::abs(getTinyInt())); break;
    case VALUE_TYPE_SMALLINT:
        retval.getSmallInt() = static_cast<int16_t>(std::abs(getSmallInt())); break;
    case VALUE_TYPE_INTEGER:
        retval.getInteger() = std::abs(getInteger()); break;
    case VALUE_TYPE_BIGINT:
        retval.getBigInt() = std::abs(getBigInt()); break;
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::abs(getDouble()); break;
    case VALUE_TYPE_DECIMAL: {
        retval.getDecimal() = getDecimal();
        retval.getDecimal().Abs(); // updates in place!
    }
    break;
    case VALUE_TYPE_TIMESTAMP:
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}

/** implement the SQL FLOOR function for all numeric values */
template<> inline Value Value::callUnary<FUNC_FLOOR>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    Value retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*floor() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit).*/
    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::floor(getDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = getDecimal();
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
        retval.getDecimal() = whole;
    }
    break;
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}


/** implement the SQL CEIL function for all numeric values */
template<> inline Value Value::callUnary<FUNC_CEILING>() const {
    if (isNull()) {
        return *this;
    }
    const ValueType type = getValueType();
    Value retval(type);
    switch(type) {

    case VALUE_TYPE_TINYINT:
    case VALUE_TYPE_SMALLINT:
    case VALUE_TYPE_INTEGER:
    case VALUE_TYPE_BIGINT:
        return *this;

    /*ceil() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit). VoltDB INTEGER is 32-bit, BIGINT is 64-bit,
      so for TINYINT (8-bit) and SMALLINT (16-bit), we need to cast.*/

    case VALUE_TYPE_DOUBLE:
        retval.getDouble() = std::ceil(getDouble());
        break;
    case VALUE_TYPE_DECIMAL: {
        TTInt scaledValue = getDecimal();
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
        retval.getDecimal() = whole;
    }
    break;
    default:
        throwCastSQLException (type, VALUE_TYPE_FOR_DIAGNOSTICS_ONLY_NUMERIC);
        break;
    }
    return retval;
}



/** implement the SQL SQRT function for all numeric values */
template<> inline Value Value::callUnary<FUNC_SQRT>() const {
    if (isNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    /*sqrt() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
      and long double (128-bit) if input is long double (128-bit).*/
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::sqrt(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function SQRT");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL EXP function for all numeric values */
template<> inline Value Value::callUnary<FUNC_EXP>() const {
    if (isNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    //exp() in C++ returns double (64-bits) if input is double, float (32-bit) if input is float,
    //and long double (128-bit) if input is long double (128-bit).
    double exponentValue = castAsDoubleAndGetValue();
    double resultDouble = std::exp(exponentValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function EXP");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL LOG/LN function for all numeric values */
template<> inline Value Value::callUnary<FUNC_LN>() const {
    if (isNull()) {
        return *this;
    }
    Value retval(VALUE_TYPE_DOUBLE);
    double inputValue = castAsDoubleAndGetValue();
    double resultDouble = std::log(inputValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function LN");
    retval.getDouble() = resultDouble;
    return retval;
}


/** implement the SQL POWER function for all numeric values */
template<> inline Value Value::call<FUNC_POWER>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    Value retval(VALUE_TYPE_DOUBLE);
    const Value& base = arguments[0];
    const Value& exponent = arguments[1];

    if (base.isNull()) {
        return base;
    }
    if (exponent.isNull())
    {
      return exponent;
    }
    double baseValue = base.castAsDoubleAndGetValue();
    double exponentValue = exponent.castAsDoubleAndGetValue();
    double resultDouble = std::pow(baseValue, exponentValue);
    throwDataExceptionIfInfiniteOrNaN(resultDouble, "function POWER");
    retval.getDouble() = resultDouble;
    return retval;
}

/**
 * FYI, http://stackoverflow.com/questions/7594508/modulo-operator-with-negative-values
 *
 * It looks like having any negative operand results in undefined behavior,
 * meaning different C++ compilers could get different answers here.
 * In C++2003, the modulo operator (%) is implementation defined.
 * In C++2011 the policy is slavish devotion to Fortran semantics. This makes sense because,
 * apparently, all the current hardware uses Fortran semantics anyway. So, even if it's implementation
 * defined it's likely to be implementation defined in the same way.
 *
 * FYI, Fortran semantics: https://gcc.gnu.org/onlinedocs/gfortran/MOD.html
 * It has the same semantics with C99 as: (a / b) * b + MOD(a,b)  == a
 */
template<> inline Value Value::call<FUNC_MOD>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& base = arguments[0];
    const Value& divisor = arguments[1];

    const ValueType baseType = base.getValueType();
    const ValueType divisorType = divisor.getValueType();

    // planner should guard against any invalid number type
    if (!isNumeric(baseType) || !isNumeric(divisorType)) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-numeric type for SQL MOD function");
    }

    bool areAllIntegralType = isIntegralType(baseType) && isIntegralType(divisorType);

    if (! areAllIntegralType) {
        throw SQLException(SQLException::dynamic_sql_error, "unsupported non-integral type for SQL MOD function");
    }
    if (base.isNull() || divisor.isNull()) {
        return getNullValue(VALUE_TYPE_BIGINT);
    } else if (divisor.castAsBigIntAndGetValue() == 0) {
        throw SQLException(SQLException::data_exception_division_by_zero, "division by zero");
    }

    Value retval(divisorType);
    int64_t baseValue = base.castAsBigIntAndGetValue();
    int64_t divisorValue = divisor.castAsBigIntAndGetValue();

    int64_t result = std::abs(baseValue) % std::abs(divisorValue);
    if (baseValue < 0) {
        result *= -1;
    }

    return getBigIntValue(result);
}

}  // End expression namespace
}  // End peloton namespace

