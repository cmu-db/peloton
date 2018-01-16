//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// old_engine_string_functions.cpp
//
// Identification: src/function/old_engine_string_functions.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "function/old_engine_string_functions.h"

#include <algorithm>
#include <cctype>
#include <string>

#include "function/string_functions.h"
#include "type/value_factory.h"

#define NextByte(p, plen) ((p)++, (plen)--)

namespace peloton {
namespace function {

// ASCII code of the first character of the argument.
type::Value OldEngineStringFunctions::Ascii(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  uint32_t ret = StringFunctions::Ascii(args[0].GetAs<const char *>(),
                                        args[0].GetLength());
  return type::ValueFactory::GetIntegerValue(ret);
}

type::Value OldEngineStringFunctions::Like(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  bool ret =
      StringFunctions::Like(args[0].GetAs<const char *>(), args[0].GetLength(),
                            args[1].GetAs<const char *>(), args[1].GetLength());
  return type::ValueFactory::GetBooleanValue(ret);
}

// Get Character from integer
type::Value OldEngineStringFunctions::Chr(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  int32_t val = args[0].GetAs<int32_t>();
  std::string str(1, static_cast<char>(val));
  return type::ValueFactory::GetVarcharValue(str);
}

// substring
type::Value OldEngineStringFunctions::Substr(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 3);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  int32_t from = args[1].GetAs<int32_t>() - 1;
  int32_t len = args[2].GetAs<int32_t>();

  return type::ValueFactory::GetVarcharValue(str.substr(from, len));
}

// Number of characters in string
type::Value OldEngineStringFunctions::CharLength(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (type::ValueFactory::GetIntegerValue(len));
}

// Concatenate two strings
type::Value OldEngineStringFunctions::Concat(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString() + args[1].ToString();
  return (type::ValueFactory::GetVarcharValue(str));
}

// Number of bytes in string
type::Value OldEngineStringFunctions::OctetLength(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (type::ValueFactory::GetIntegerValue(len));
}

// Repeat string the specified number of times
type::Value OldEngineStringFunctions::Repeat(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  int32_t num = args[1].GetAs<int32_t>();

  std::string ret = "";

  while (num > 0) {
    if (num % 2) {
      ret += str;
    }
    if (num > 1) {
      str += str;
    }
    num >>= 1;
  }
  return (type::ValueFactory::GetVarcharValue(ret));
}

// Replace all occurrences in string of substring from with substring to
type::Value OldEngineStringFunctions::Replace(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 3);
  if (args[0].IsNull() || args[1].IsNull() || args[2].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  std::string from = args[1].ToString();
  std::string to = args[2].ToString();
  size_t pos = 0;
  while ((pos = str.find(from, pos)) != std::string::npos) {
    str.replace(pos, from.length(), to);
    pos += to.length();
  }
  return (type::ValueFactory::GetVarcharValue(str));
}

// Remove the longest string containing only characters from characters
// from the start of string
type::Value OldEngineStringFunctions::LTrim(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  auto ret = StringFunctions::LTrim(
      args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
}

// Remove the longest string containing only characters from characters
// from the end of string
type::Value OldEngineStringFunctions::RTrim(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  auto ret = StringFunctions::RTrim(
      args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
}

type::Value OldEngineStringFunctions::Trim(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  return BTrim({args[0], type::ValueFactory::GetVarcharValue(" ")});
}

// Remove the longest string consisting only of characters in characters from
// the start and end of string
type::Value OldEngineStringFunctions::BTrim(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  auto ret = StringFunctions::BTrim(
      args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
      args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
}

// The length of the string
type::Value OldEngineStringFunctions::Length(
    const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  uint32_t ret = StringFunctions::Length(args[0].GetAs<const char *>(),
                                         args[0].GetLength());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
