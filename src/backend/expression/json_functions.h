//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// json_functions.h
//
// Identification: src/backend/expression/json_functions.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <cstring>
#include <string>
#include <sstream>
#include <algorithm>

#include "jsoncpp/jsoncpp.h"
#include "jsoncpp/jsoncpp-forwards.h"

namespace peloton {
namespace expression {

/** a path node is either a field name or an array index */
struct JsonPathNode {
    JsonPathNode(int32_t arrayIndex) : m_arrayIndex(arrayIndex) {}
    JsonPathNode(const char* field) : m_arrayIndex(-1), m_field(field) {}

    int32_t m_arrayIndex;
    std::string m_field;
};

/** representation of a JSON document that can be accessed and updated via
    our path syntax */
class JsonDocument {
public:
    JsonDocument(const char* docChars, int32_t lenDoc) : m_head(NULL), m_tail(NULL) {
        if (docChars == NULL) {
            // null documents have null everything, but they turn into objects/arrays
            // if we try to set their properties
            m_doc = Json::Value::null;
        } else if (!m_reader.parse(docChars, docChars + lenDoc, m_doc)) {
            // we have something real, but it isn't JSON
            ThrowJsonFormattingError();
        }
    }

    std::string value() { return m_writer.write(m_doc); }

    bool Get(const char* pathChars, int32_t lenPath, std::string& serializedValue) {
        if (m_doc.isNull()) {
            return false;
        }

        // Get and traverse the path
        std::vector<JsonPathNode> path = resolveJsonPath(pathChars, lenPath);
        const Json::Value* node = &m_doc;
        for (std::vector<JsonPathNode>::const_iterator cit = path.begin(); cit != path.end(); ++cit) {
            const JsonPathNode& pathNode = *cit;
            if (pathNode.m_arrayIndex != -1) {
                // can't access an array index of something that isn't an array
                if (!node->isArray()) {
                    return false;
                }
                int32_t arrayIndex = pathNode.m_arrayIndex;
                if (arrayIndex == ARRAY_TAIL) {
                    unsigned int arraySize = node->size();
                    arrayIndex = arraySize > 0 ? arraySize - 1 : 0;
                }
                node = &((*node)[arrayIndex]);
                if (node->isNull()) {
                    return false;
                }
            } else {
                // this is a field. only objects have fields
                if (!node->isObject()) {
                    return false;
                }
                node = &((*node)[pathNode.m_field]);
                if (node->isNull()) {
                    return false;
                }
            }
        }

        // return the string representation of what we have obtained
        if (node->isConvertibleTo(Json::stringValue)) {
            // 'append' is to standardize that there's something to remove. quicker
            // than substr on the other one, which incurs an extra copy
            serializedValue = node->asString().append(1, '\n');
        } else {
            serializedValue = m_writer.write(*node);
        }
        return true;
    }

    void set(const char* pathChars, int32_t lenPath, const char* valueChars, int32_t lenValue) {
        // translate database nulls into JSON nulls, because that's really all that makes
        // any semantic sense. otherwise, parse the value as JSON
        Json::Value value;
        if (lenValue <= 0) {
            value = Json::Value::null;
        } else if (!m_reader.parse(valueChars, valueChars + lenValue, value)) {
            ThrowJsonFormattingError();
        }

        std::vector<JsonPathNode> path = resolveJsonPath(pathChars, lenPath, true /*enforceArrayIndexLimitForSet*/);
        // the non-const version of the Json::Value [] operator creates a new, null node on attempted
        // access if none already exists
        Json::Value* node = &m_doc;
        for (std::vector<JsonPathNode>::const_iterator cit = path.begin(); cit != path.end(); ++cit) {
            const JsonPathNode& pathNode = *cit;
            if (pathNode.m_arrayIndex != -1) {
                if (!node->isNull() && !node->isArray()) {
                    // no-op if the update is impossible, I guess?
                    return;
                }
                int32_t arrayIndex = pathNode.m_arrayIndex;
                if (arrayIndex == ARRAY_TAIL) {
                    arrayIndex = node->size();
                }
                // Get or create the specified node
                node = &((*node)[arrayIndex]);
            } else {
                if (!node->isNull() && !node->isObject()) {
                    return;
                }
                node = &((*node)[pathNode.m_field]);
            }
        }
        *node = value;
    }

private:
    Json::Value m_doc;
    Json::Reader m_reader;
    Json::FastWriter m_writer;

    const char* m_head;
    const char* m_tail;
    int32_t m_pos;

    static const int32_t ARRAY_TAIL = -10;

    /** parse our path to its vector representation */
    std::vector<JsonPathNode> resolveJsonPath(const char* pathChars, int32_t lenPath,
                                              bool enforceArrayIndexLimitForSet = false) {
        std::vector<JsonPathNode> path;
        // NULL path refers directly to the doc root
        if (pathChars == NULL) {
            return path;
        }
        m_head = pathChars;
        m_tail = m_head + lenPath;

        m_pos = -1;
        char c;
        bool first = true;
        bool expectArrayIndex = false;
        bool expectField = false;
        char strField[lenPath];
        while (readChar(c)) {
            if (expectArrayIndex) {
                // -1 index to refer to the tail of the array
                bool neg = false;
                if (c == '-') {
                    neg = true;
                    if (!readChar(c)) {
                        ThrowInvalidPathError("Unexpected termination (unterminated array access)");
                    }
                }
                if (c < '0' || c > '9') {
                    ThrowInvalidPathError("Unexpected character in array index");
                }
                // atoi while advancing our pointer
                int64_t arrayIndex = c - '0';
                bool terminated = false;
                while (readChar(c)) {
                    if (c == ']') {
                        terminated = true;
                        break;
                    } else if (c < '0' || c > '9') {
                        ThrowInvalidPathError("Unexpected character in array index");
                    }
                    arrayIndex = 10 * arrayIndex + (c - '0');
                    if (enforceArrayIndexLimitForSet) {
                        // This 500000 is a mostly arbitrary maximum JSON array index enforced for practical
                        // purposes. We enforce this up front to avoid excessive delays, ridiculous short-term
                        // memory growth, and/or bad_alloc errors that the jsoncpp library could produce
                        // essentially for nothing since our supported JSON document columns are typiCally not
                        // wide enough to hold the string representations of arrays this large.
                        if (arrayIndex > 500000) {
                            if (neg) {
                                // other than the special '-1' case, negative indices aren't allowed
                                ThrowInvalidPathError("Array index less than -1");
                            }
                            ThrowInvalidPathError("Array index greater than the maximum allowed value of 500000");
                        }
                    } else {
                        if (arrayIndex > static_cast<int64_t>(INT32_MAX)) {
                            if (neg) {
                                // other than the special '-1' case, negative indices aren't allowed
                                ThrowInvalidPathError("Array index less than -1");
                            }
                            ThrowInvalidPathError("Array index greater than the maximum integer value");
                        }
                    }
                }
                if ( ! terminated ) {
                    ThrowInvalidPathError("Missing ']' after array index");
                }
                if (neg) {
                    // other than the special '-1' case, negative indices aren't allowed
                    if (arrayIndex != 1) {
                        ThrowInvalidPathError("Array index less than -1");
                    }
                    arrayIndex = ARRAY_TAIL;
                }
                path.push_back(static_cast<int32_t>(arrayIndex));
                expectArrayIndex = false;
            } else if (c == '[') {
                // handle the case of empty field names. for example, Getting the first element of the array
                // in { "a": { "": [ true, false ] } } would be the path 'a.[0]'
                if (expectField) {
                    path.push_back(JsonPathNode(""));
                    expectField = false;
                }
                expectArrayIndex = true;
            } else if (c == '.') {
                // a leading '.' also involves accessing the "" property of the root...
                if (expectField || first) {
                    path.push_back(JsonPathNode(""));
                }
                expectField = true;
            } else {
                expectField = false;
                // read a literal field name
                int32_t i = 0;
                do {
                    if (c == '\\') {
                        if (!readChar(c) || (c != '[' && c != ']' && c != '.' && c != '\\')) {
                            ThrowInvalidPathError("Unescaped backslash (double escaping required for path)");
                        }
                    } else if (c == '.') {
                        expectField = true;
                        break;
                    } else if (c == '[') {
                        expectArrayIndex = true;
                        break;
                    }
                    strField[i++] = c;
                } while (readChar(c));
                strField[i] = '\0';
                path.push_back(JsonPathNode(strField));
            }
            first = false;
        }
        // trailing '['
        if (expectArrayIndex) {
            ThrowInvalidPathError("Unexpected termination (unterminated array access)");
        }
        // if we're either empty or ended on a trailing '.', add an empty field name
        if (expectField || first) {
            path.push_back(JsonPathNode(""));
        }

        m_head = NULL;
        m_tail = NULL;
        return path;
    }

    bool readChar(char& c) {
        assert(m_head != NULL && m_tail != NULL);
        if (m_head == m_tail) {
            return false;
        }
        c = *m_head++;
        m_pos++;
        return true;
    }

    void ThrowInvalidPathError(const char* err) const {
        char msg[1024];
        snprintf(msg, sizeof(msg), "Invalid JSON path: %s [position %d]", err, m_pos);
        throw Exception(msg);
    }

    void ThrowJsonFormattingError() const {
        char msg[1024];
        // GetFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", m_reader.getFormatedErrorMessages().c_str());
        throw Exception(msg);
    }
};

/** implement the 2-argument SQL FIELD function */
template<> inline Value Value::Call<FUNC_VOLT_FIELD>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);

    const Value& docNVal = arguments[0];
    const Value& pathNVal = arguments[1];

    if (docNVal.IsNull()) {
        return docNVal;
    }
    if (pathNVal.IsNull()) {
        throw Exception("Invalid FIELD path argument (SQL null)");
    }

    if (docNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(docNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }
    if (pathNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(pathNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    int32_t lenDoc = docNVal.GetObjectLengthWithoutNull();
    const char* docChars = reinterpret_cast<char*>(docNVal.GetObjectValueWithoutNull());
    JsonDocument doc(docChars, lenDoc);

    int32_t lenPath = pathNVal.GetObjectLengthWithoutNull();
    const char* pathChars = reinterpret_cast<char*>(pathNVal.GetObjectValueWithoutNull());
    std::string result;
    if (doc.get(pathChars, lenPath, result)) {
        return GetTempStringValue(result.c_str(), result.length() - 1);
    }
    return GetNullStringValue();
}

/** implement the 2-argument SQL ARRAY_ELEMENT function */
template<> inline Value Value::Call<FUNC_VOLT_ARRAY_ELEMENT>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 2);

    const Value& docNVal = arguments[0];
    if (docNVal.IsNull()) {
        return GetNullStringValue();
    }
    if (docNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(docNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    const Value& indexNVal = arguments[1];
    if (indexNVal.IsNull()) {
        return GetNullStringValue();
    }
    int32_t lenDoc = docNVal.GetObjectLengthWithoutNull();
    char *docChars = reinterpret_cast<char*>(docNVal.GetObjectValueWithoutNull());
    const std::string doc(docChars, lenDoc);

    int32_t index = indexNVal.CastAsIntegerAndGetValue();

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // GetFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
        throw Exception(msg);
    }

    // only array type contains elements. objects, primitives do not
    if( ! root.isArray()) {
        return GetNullStringValue();
    }

    // Sure, root[-1].IsNull() would return true just like we want it to
    // -- but only in production with asserts turned off.
    // Turn on asserts for Debugging and you'll want this guard up front.
    // Forcing the null return for a negative index seems more consistent than crashing in Debug mode
    // or even Throwing an SQL error in any mode. It's the same handling that a too large index Gets.
    if (index < 0) {
        return GetNullStringValue();
    }

    Json::Value fieldValue = root[index];

    if (fieldValue.isNull()) {
        return GetNullStringValue();
    }

    if (fieldValue.isConvertibleTo(Json::stringValue)) {
        std::string stringValue(fieldValue.asString());
        return GetTempStringValue(stringValue.c_str(), stringValue.length());
    }

    Json::FastWriter writer;
    std::string serializedValue(writer.write(fieldValue));
    // writer always appends a trailing new line \n
    return GetTempStringValue(serializedValue.c_str(), serializedValue.length() -1);
}

/** implement the 1-argument SQL ARRAY_LENGTH function */
template<> inline Value Value::CallUnary<FUNC_VOLT_ARRAY_LENGTH>() const {

    if (IsNull()) {
        return GetNullValue(VALUE_TYPE_INTEGER);
    }
    if (GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(GetValueType(), VALUE_TYPE_VARCHAR);
    }

    int32_t lenDoc = GetObjectLengthWithoutNull();
    char *docChars = reinterpret_cast<char*>(GetObjectValueWithoutNull());
    const std::string doc(docChars, lenDoc);

    Json::Value root;
    Json::Reader reader;

    if( ! reader.parse(doc, root)) {
        char msg[1024];
        // GetFormatedErrorMessages returns concise message about location
        // of the error rather than the malformed document itself
        snprintf(msg, sizeof(msg), "Invalid JSON %s", reader.getFormatedErrorMessages().c_str());
        throw Exception(msg);
    }

    // only array type contains indexed elements. objects, primitives do not
    if( ! root.isArray()) {
        return GetNullValue(VALUE_TYPE_INTEGER);
    }

    Value result(VALUE_TYPE_INTEGER);
    int32_t size = static_cast<int32_t>(root.size());
    result.GetInteger() = size;
    return result;
}

/** implement the 3-argument SQL SET_FIELD function */
template<> inline Value Value::Call<FUNC_VOLT_SET_FIELD>(const std::vector<Value>& arguments) {
    assert(arguments.size() == 3);

    const Value& docNVal = arguments[0];
    const Value& pathNVal = arguments[1];
    const Value& valueNVal = arguments[2];

    if (docNVal.IsNull()) {
        return docNVal;
    }
    if (pathNVal.IsNull()) {
        throw Exception("Invalid SET_FIELD path argument (SQL null)");
    }
    if (valueNVal.IsNull()) {
        throw Exception("Invalid SET_FIELD value argument (SQL null)");
    }

    if (docNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(docNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    if (pathNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(pathNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    if (valueNVal.GetValueType() != VALUE_TYPE_VARCHAR) {
        ThrowCastSQLException(valueNVal.GetValueType(), VALUE_TYPE_VARCHAR);
    }

    int32_t lenDoc = docNVal.GetObjectLengthWithoutNull();
    const char* docChars = reinterpret_cast<char*>(docNVal.GetObjectValueWithoutNull());
    JsonDocument doc(docChars, lenDoc);

    int32_t lenPath = pathNVal.GetObjectLengthWithoutNull();
    const char* pathChars = reinterpret_cast<char*>(pathNVal.GetObjectValueWithoutNull());
    int32_t lenValue = valueNVal.GetObjectLengthWithoutNull();
    const char* valueChars = reinterpret_cast<char*>(valueNVal.GetObjectValueWithoutNull());
    try {
        doc.set(pathChars, lenPath, valueChars, lenValue);
        std::string value = doc.value();
        return GetTempStringValue(value.c_str(), value.length() - 1);
    } catch (std::bad_alloc& too_large) {
        std::string pathForDiagnostic(pathChars, lenPath);
        throw Exception("Insufficient memory for SET_FIELD operation with path argument: %s" +
                        pathForDiagnostic);
    }
}

}  // End expression namespace
}  // End peloton namespace
