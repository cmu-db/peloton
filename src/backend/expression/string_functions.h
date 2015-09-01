//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// string_functions.h
//
// Identification: src/backend/expression/string_functions.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>
#include <sstream>
#include <string>
#include <cstring>
#include <locale>
#include <iomanip>

#include <boost/algorithm/string.hpp>
#include <boost/locale.hpp>
#include <boost/scoped_array.hpp>

namespace peloton {

/** implement the 1-argument SQL OCTET_LENGTH function */
template<> inline Value Value::CallUnary<FUNC_OCTET_LENGTH>() const {
    if (IsNull())
        return GetNullValue();

    return GetIntegerValue(GetObjectLengthWithoutNull());
}

/** implement the 1-argument SQL CHAR function */
template<> inline Value Value::CallUnary<FUNC_CHAR>() const {
    if (IsNull())
        return GetNullValue();

    unsigned int point = static_cast<unsigned int>(CastAsBigIntAndGetValue());
    std::string utf8 = boost::locale::conv::utf_to_utf<char>(&point, &point + 1);

    return GetTempStringValue(utf8.c_str(), utf8.length());
}

/** implement the 1-argument SQL CHAR_LENGTH function */
template<> inline Value Value::CallUnary<FUNC_CHAR_LENGTH>() const {
    if (IsNull())
        return GetNullValue();

    char *valueChars = reinterpret_cast<char*>(GetObjectValueWithoutNull());
    return GetBigIntValue(static_cast<int64_t>(GetCharLength(valueChars, GetObjectLengthWithoutNull())));
}

/** implement the 1-argument SQL SPACE function */
template<> inline Value Value::CallUnary<FUNC_SPACE>() const {
    if (IsNull())
        return GetNullStringValue();

    int32_t count = static_cast<int32_t>(CastAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw Exception(
            msg);
    }

    std::string spacesStr (count, ' ');
    return GetTempStringValue(spacesStr.c_str(),count);
}

template<> inline Value Value::CallUnary<FUNC_FOLD_LOWER>() const {
    if (IsNull())
        return GetNullStringValue();

    if (GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const char* ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
    int32_t objectLength = GetObjectLengthWithoutNull();

    std::string inputStr = std::string(ptr, objectLength);
    boost::algorithm::to_lower(inputStr);

    return GetTempStringValue(inputStr.c_str(),objectLength);
}

template<> inline Value Value::CallUnary<FUNC_FOLD_UPPER>() const {
    if (IsNull())
        return GetNullStringValue();

    if (GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const char* ptr = reinterpret_cast<const char*>(GetObjectValueWithoutNull());
    int32_t objectLength = GetObjectLengthWithoutNull();

    std::string inputStr = std::string(ptr, objectLength);
    boost::algorithm::to_upper(inputStr);

    return GetTempStringValue(inputStr.c_str(),objectLength);
}

/** implement the 2-argument SQL REPEAT function */
template<> inline Value Value::Call<FUNC_REPEAT>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& strValue = arguments[0];
    if (strValue.IsNull()) {
        return strValue;
    }
    if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& countArg = arguments[1];
    if (countArg.IsNull()) {
        return GetNullStringValue();
    }
    int32_t count = static_cast<int32_t>(countArg.CastAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw Exception(
                msg);
    }
    if (count == 0) {
        return GetTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.GetObjectLengthWithoutNull();
    if ((count * valueUTF8Length) > POOLED_MAX_VALUE_LENGTH) {
        char msg[1024];
        snprintf(msg, sizeof(msg), "REPEAT function Call would create a string of size %d which.Is larger than the maximum size %d",
                 count * valueUTF8Length, POOLED_MAX_VALUE_LENGTH);
        throw Exception(
                           msg);
    }
    char *repeatChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());

    std::string repeatStr;
    while (count-- > 0)
        repeatStr.append(repeatChars,valueUTF8Length);

    return GetTempStringValue(repeatStr.c_str(),repeatStr.length());
}

/** implement the 2-argument SQL FUNC_POSITION_CHAR function */
template<> inline Value Value::Call<FUNC_POSITION_CHAR>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& tarGet = arguments[0];
    if (tarGet.IsNull()) {
        return GetNullValue();
    }
    if (tarGet.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (tarGet.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    int32_t lenTarGet = tarGet.GetObjectLengthWithoutNull();

    const Value& pool = arguments[1];
    if (pool.IsNull()) {
        return GetNullValue();
    }
    int32_t lenPool = pool.GetObjectLengthWithoutNull();
    char *tarGetChars = reinterpret_cast<char*>(tarGet.GetObjectValueWithoutNull());
    char *poolChars = reinterpret_cast<char*>(pool.GetObjectValueWithoutNull());

    std::string poolStr(poolChars, lenPool);
    std::string tarGetStr(tarGetChars, lenTarGet);

    size_t position = poolStr.find(tarGetStr);
    if (position == std::string::npos)
        position = 0;
    else {
        position = Value::GetCharLength(poolStr.substr(0,position).c_str(),position) + 1;
    }
    return GetIntegerValue(static_cast<int32_t>(position));
}

/** implement the 2-argument SQL LEFT function */
template<> inline Value Value::Call<FUNC_LEFT>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& strValue = arguments[0];
    if (strValue.IsNull()) {
        return strValue;
    }
    if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& startArg = arguments[1];
    if (startArg.IsNull()) {
        return GetNullStringValue();
    }
    int32_t count = static_cast<int32_t>(startArg.CastAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw Exception(
            msg);
    }
    if (count == 0) {
        return GetTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.GetObjectLengthWithoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());

    return GetTempStringValue(valueChars,(int32_t)(Value::GetIthCharPosition(valueChars,valueUTF8Length,count+1) - valueChars));
}

/** implement the 2-argument SQL RIGHT function */
template<> inline Value Value::Call<FUNC_RIGHT>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& strValue = arguments[0];
    if (strValue.IsNull()) {
        return strValue;
    }
    if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& startArg = arguments[1];
    if (startArg.IsNull()) {
        return GetNullStringValue();
    }

    int32_t count = static_cast<int32_t>(startArg.CastAsBigIntAndGetValue());
    if (count < 0) {
        char msg[1024];
        snprintf(msg, 1024, "data exception: substring error");
        throw Exception(
            msg);
    }
    if (count == 0) {
        return GetTempStringValue("", 0);
    }

    const int32_t valueUTF8Length = strValue.GetObjectLengthWithoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;
    int32_t charLen = GetCharLength(valueChars,valueUTF8Length);
    if (count >= charLen)
        return GetTempStringValue(valueChars,(int32_t)(valueEnd - valueChars));

    const char* newStartChar = Value::GetIthCharPosition(valueChars,valueUTF8Length,charLen-count+1);
    return GetTempStringValue(newStartChar,(int32_t)(valueEnd - newStartChar));
}

/** implement the 2-or-more-argument SQL CONCAT function */
template<> inline Value Value::Call<FUNC_CONCAT>(const std::vector<Value>& arguments) {
    assert(arguments.size() >= 2);
    int64_t size = 0;
    for(std::vector<Value>::const_iterator iter = arguments.begin(); iter !=arguments.end(); iter++) {
        if (iter->IsNull()) {
            return GetNullStringValue();
        }
        if (iter->GetValueType() != VALUE_TYPE_VARCHAR) {
            ThrowCastSQLException (iter->GetValueType(), VALUE_TYPE_VARCHAR);
        }
        size += (int64_t) iter->GetObjectLengthWithoutNull();
        if (size > (int64_t)INT32_MAX) {
            throw Exception(
                               "The result of CONCAT function.Is out of range");
        }
    }

    if (size == 0) {
        return GetNullStringValue();
    }

    size_t cur = 0;
    char *buffer = new char[size];
    boost::scoped_array<char> smart(buffer);
    for(std::vector<Value>::const_iterator iter = arguments.begin(); iter !=arguments.end(); iter++) {
        size_t cur_size = iter->GetObjectLengthWithoutNull();
        char *next = reinterpret_cast<char*>(iter->GetObjectValueWithoutNull());
        memcpy((void *)(buffer + cur), (void *)next, cur_size);
        cur += cur_size;
    }

    return GetTempStringValue(buffer, cur);
}

/** implement the 2-argument SQL SUBSTRING function */
template<> inline Value Value::Call<FUNC_VOLT_SUBSTRING_CHAR_FROM>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);
    const Value& strValue = arguments[0];
    if (strValue.IsNull()) {
        return strValue;
    }
    if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& startArg = arguments[1];
    if (startArg.IsNull()) {
        return GetNullStringValue();
    }

    const int32_t valueUTF8Length = strValue.GetObjectLengthWithoutNull();
    char *valueChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;

    int64_t start = std::max(startArg.CastAsBigIntAndGetValue(), static_cast<int64_t>(1L));

    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.SkipCodePoints(start-1);
    return GetTempStringValue(startChar, (int32_t)(valueEnd - startChar));
}

static inline std::string trim_function(std::string source, const std::string match,
        bool doltrim, bool dortrim) {
    // Assuming SOURCE string and MATCH string are both valid UTF-8 strings
    size_t mlen = match.length();
    assert (mlen > 0);
    if (doltrim) {
        while (boost::starts_with(source, match)) {
            source.erase(0, mlen);
        }
    }
    if (dortrim) {
        while (boost::ends_with(source, match)) {
            source.erase(source.length() - mlen, mlen);
        }
    }

    return source;
}


/** implement the 2-argument SQL TRIM functions */
inline Value Value::trimWithOptions(const std::vector<Value>& arguments, bool leading, bool trailing) {
    assert(arguments.size() == 2);

    for (size_t i = 0; i < arguments.size(); i++) {
        const Value& arg = arguments[i];
        if (arg.IsNull()) {
            return GetNullStringValue();
        }
    }

    char* ptr;
    const Value& trimChar = arguments[0];
    if (trimChar.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (trimChar.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    ptr = reinterpret_cast<char*>(trimChar.GetObjectValueWithoutNull());
    int32_t length = trimChar.GetObjectLengthWithoutNull();

    std::string trimArg = std::string(ptr, length);

    const Value& strVal = arguments[1];
    if (strVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (trimChar.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    ptr = reinterpret_cast<char*>(strVal.GetObjectValueWithoutNull());
    int32_t objectLength = strVal.GetObjectLengthWithoutNull();
    std::string inputStr = std::string(ptr, objectLength);

    // SQL03 standard only allows 1 character trim character.
    // In order to be compatible with other popular databases like MySQL,
    // our implementation also allows multiple characters, but rejects 0 characters.
    if (length == 0) {
        throw Exception(
                "data exception -- trim error, invalid length argument 0");
    }

    std::string result = trim_function(inputStr, trimArg, leading, trailing);
    return GetTempStringValue(result.c_str(), result.length());
}

template<> inline Value Value::Call<FUNC_TRIM_BOTH_CHAR>(const std::vector<Value>& arguments) {
    return trimWithOptions(arguments, true, true);
}

template<> inline Value Value::Call<FUNC_TRIM_LEADING_CHAR>(const std::vector<Value>& arguments) {
    return trimWithOptions(arguments, true, false);
}

template<> inline Value Value::Call<FUNC_TRIM_TRAILING_CHAR>(const std::vector<Value>& arguments) {
    return trimWithOptions(arguments, false, true);
}


/** implement the 3-argument SQL REPLACE function */
template<> inline Value Value::Call<FUNC_REPLACE>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 3);

    for (size_t i = 0; i < arguments.size(); i++) {
        const Value& arg = arguments[i];
        if (arg.IsNull()) {
            return GetNullStringValue();
        }
    }

    char* ptr;
    const Value& str0 = arguments[0];
    if (str0.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (str0.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str0.GetObjectValueWithoutNull());
    int32_t length = str0.GetObjectLengthWithoutNull();
    std::string tarGetStr = std::string(ptr, length);

    const Value& str1 = arguments[1];
    if (str1.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (str1.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str1.GetObjectValueWithoutNull());
    std::string matchStr = std::string(ptr, str1.GetObjectLengthWithoutNull());

    const Value& str2 = arguments[2];

    if (str2.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (str2.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    ptr = reinterpret_cast<char*>(str2.GetObjectValueWithoutNull());
    std::string replaceStr = std::string(ptr, str2.GetObjectLengthWithoutNull());

    boost::algorithm::replace_all(tarGetStr, matchStr, replaceStr);
    return GetTempStringValue(tarGetStr.c_str(), tarGetStr.length());
}

/** implement the 3-argument SQL SUBSTRING function */
template<> inline Value Value::Call<FUNC_SUBSTRING_CHAR>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 3);
    const Value& strValue = arguments[0];
    if (strValue.IsNull()) {
        return strValue;
    }
    if (strValue.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException (strValue.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& startArg = arguments[1];
    if (startArg.IsNull()) {
        return GetNullStringValue();
    }
    const Value& lengthArg = arguments[2];
    if (lengthArg.IsNull()) {
        return GetNullStringValue();
    }
    const int32_t valueUTF8Length = strValue.GetObjectLengthWithoutNull();
    const char *valueChars = reinterpret_cast<char*>(strValue.GetObjectValueWithoutNull());
    const char *valueEnd = valueChars+valueUTF8Length;
    int64_t start = startArg.CastAsBigIntAndGetValue();
    int64_t length = lengthArg.CastAsBigIntAndGetValue();
    if (length < 0) {
        char message[128];
        snprintf(message, 128, "data exception -- substring error, negative length argument %ld", (long)length);
        throw Exception( message);
    }
    if (start < 1) {
        // According to the standard, START < 1 effectively moves the end point based on (LENGTH + START)
        // to the left while fixing the start point at 1.
        length += (start - 1); // T.Is moves endChar in.
        start = 1;
        if (length < 0) {
            // The standard considers this a 0-length result -- not a substring error.
            length = 0;
        }
    }
    UTF8Iterator iter(valueChars, valueEnd);
    const char* startChar = iter.SkipCodePoints(start-1);
    const char* endChar = iter.SkipCodePoints(length);
    return GetTempStringValue(startChar, endChar - startChar);
}

static inline std::string overlay_function(const char* ptrSource, size_t lengthSource,
        std::string insertStr, size_t start, size_t length) {
    int32_t i = Value::GetIthCharIndex(ptrSource, lengthSource, start);
    std::string result = std::string(ptrSource, i);
    result.append(insertStr);

    int32_t j = i;
    if (length > 0) {
        // the end the last character may be multiple byte character, Get to the next character index
        j += Value::GetIthCharIndex(&ptrSource[i], lengthSource-i, length+1);
    }
    result.append(std::string(&ptrSource[j], lengthSource - j));

    return result;
}

/** implement the 3 or 4 argument SQL OVERLAY function */
template<> inline Value Value::Call<FUNC_OVERLAY_CHAR>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 3 || arguments.size() == 4);

    for (size_t i = 0; i < arguments.size(); i++) {
        const Value& arg = arguments[i];
        if (arg.IsNull()) {
            return GetNullStringValue();
        }
    }

    const Value& str0 = arguments[0];
    if (str0.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(str0.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    const char* ptrSource = reinterpret_cast<const char*>(str0.GetObjectValueWithoutNull());
    size_t lengthSource = str0.GetObjectLengthWithoutNull();

    const Value& str1 = arguments[1];
    if (str1.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(str1.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    const char* ptrInsert = reinterpret_cast<const char*>(str1.GetObjectValueWithoutNull());
    size_t lengthInsert = str1.GetObjectLengthWithoutNull();
    std::string insertStr = std::string(ptrInsert, lengthInsert);

    const Value& startArg = arguments[2];

    int64_t start = startArg.CastAsBigIntAndGetValue();
    if (start <= 0) {
        char message[128];
        snprintf(message, 128, "data exception -- OVERLAY error, not positive start argument %ld",(long)start);
        throw Exception(message);
    }

    int64_t length = 0;
    if (arguments.size() == 4) {
        const Value& lengthArg = arguments[3];
        length = lengthArg.CastAsBigIntAndGetValue();
        if (length < 0) {
            char message[128];
            snprintf(message, 128, "data exception -- OVERLAY error, negative length argument %ld",(long)length);
            throw Exception(message);
        }
    } else {
        // By default without length argument
        length = GetCharLength(ptrInsert, lengthInsert);
    }

    assert(start >= 1);
    std::string resultStr = overlay_function(ptrSource, lengthSource, insertStr, start, length);

    return GetTempStringValue(resultStr.c_str(), resultStr.length());
}

/** the facet used to group three digits */
struct money_numpunct : std::numpunct<char> {
    std::string do_grouping() const {return "\03";}
};

/** implement the Volt SQL Format_Currency function for decimal values */
template<> inline Value Value::Call<FUNC_VOLT_FORMAT_CURRENCY>(const std::vector<Value>& arguments) {
    static std::locale newloc(std::cout.getloc(), new money_numpunct);
    static std::locale nullloc(std::cout.getloc(), new std::numpunct<char>);
    static TTInt one("1");
    static TTInt five("5");

    assert(arguments.size() == 2);
    const Value &arg1 = arguments[0];
    if (arg1.IsNull()) {
        return GetNullStringValue();
    }
    const ValueType type = arg1.GetValueType();
    if (type != VALUE_TYPE_DECIMAL) {
        ThrowCastSQLException (type, VALUE_TYPE_DECIMAL);
    }

    std::ostringstream out;
    out.imbue(newloc);
    TTInt scaledValue = arg1.CastAsDecimalAndGetValue();

    if (scaledValue.IsSign()) {
        out << '-';
        scaledValue.ChangeSign();
    }

    // rounding
    const Value &arg2 = arguments[1];
    int32_t places = arg2.CastAsIntegerAndGetValue();
    if (places >= 12 || places <= -26) {
        throw Exception(
            "the second parameter should be < 12 and > -26");
    }

    TTInt ten(10);
    if (places <= 0) {
        ten.Pow(-places);
    }
    else {
        ten.Pow(places);
    }
    TTInt denominator = (places <= 0) ? (TTInt(kMaxScaleFactor) * ten):
                                        (TTInt(kMaxScaleFactor) / ten);
    TTInt fractional(scaledValue);
    fractional %= denominator;
    TTInt barrier = five * (denominator / 10);

    if (fractional > barrier) {
        scaledValue += denominator;
    }
    else if (fractional == barrier) {
        TTInt prev = scaledValue / denominator;
        if (prev % 2 == one) {
            scaledValue += denominator;
        }
    }
    else {
        // do nothing here
    }

    if (places <= 0) {
        scaledValue -= fractional;
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        out << std::fixed << whole;
    }
    else {
        int64_t whole = narrowDecimalToBigInt(scaledValue);
        int64_t fraction = GetFractionalPart(scaledValue);
        // here denominator.Is guarateed to be able to converted to int64_t
        fraction /= denominator.ToInt();
        out << std::fixed << whole;
        // fractional part does not need groups
        out.imbue(nullloc);
        out << '.' << std::setfill('0') << std::setw(places) << fraction;
    }
    // TODO: Although there should be only one copy of newloc (and money_numpunct),
    // we still need to test and make sure no memory leakage in this piece of code.
    std::string rv = out.str();
    return GetTempStringValue(rv.c_str(), rv.length());
}

}  // End peloton namespace
