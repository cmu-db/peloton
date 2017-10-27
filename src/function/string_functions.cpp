//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.cpp
//
// Identification: src/function/string_functions.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "function/string_functions.h"
#include "type/value_factory.h"

namespace peloton {
namespace function {

uint32_t StringFunctions::Ascii(const char *str, uint32_t length) {
  PL_ASSERT(str != nullptr);
  return length <= 1 ? 0 : static_cast<uint32_t>(str[0]);
}

// ASCII code of the first character of the argument.
type::Value StringFunctions::_Ascii(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  uint32_t ret = Ascii(args[0].GetAs<const char *>(), args[0].GetLength());
  return type::ValueFactory::GetIntegerValue(ret);
}

// Get Character from integer
type::Value StringFunctions::Chr(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  int32_t val = args[0].GetAs<int32_t>();
  std::string str(1, static_cast<char>(val));
  return type::ValueFactory::GetVarcharValue(str);
}

// substring
type::Value StringFunctions::Substr(const std::vector<type::Value> &args) {
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
type::Value StringFunctions::CharLength(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (type::ValueFactory::GetIntegerValue(len));
}

// Concatenate two strings
type::Value StringFunctions::Concat(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString() + args[1].ToString();
  return (type::ValueFactory::GetVarcharValue(str));
}

// Number of bytes in string
type::Value StringFunctions::OctetLength(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  std::string str = args[0].ToString();
  int32_t len = str.length();
  return (type::ValueFactory::GetIntegerValue(len));
}

// Repeat string the specified number of times
type::Value StringFunctions::Repeat(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  int32_t num = args[1].GetAs<int32_t>();
  std::string ret = "";
  while (num--) {
    ret = ret + str;
  }
  return (type::ValueFactory::GetVarcharValue(ret));
}

// Replace all occurrences in string of substring from with substring to
type::Value StringFunctions::Replace(const std::vector<type::Value> &args) {
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
type::Value StringFunctions::LTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args[0].ToString();
  std::string from = args[1].ToString();
  size_t pos = 0;
  bool erase = 0;
  while (from.find(str[pos]) != std::string::npos) {
    erase = 1;
    pos++;
  }
  if (erase) str.erase(0, pos);
  return (type::ValueFactory::GetVarcharValue(str));
}

// Remove the longest string containing only characters from characters
// from the end of string
type::Value StringFunctions::RTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args.at(0).ToString();
  std::string from = args.at(1).ToString();
  if (str.length() == 0) return (type::ValueFactory::GetVarcharValue(""));
  size_t pos = str.length() - 1;
  bool erase = 0;
  while (from.find(str[pos]) != std::string::npos) {
    erase = 1;
    pos--;
  }
  if (erase) str.erase(pos + 1, str.length() - pos - 1);
  return (type::ValueFactory::GetVarcharValue(str));
}

// Remove the longest string consisting only of characters in characters
// from the start and end of string
type::Value StringFunctions::BTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }
  std::string str = args.at(0).ToString();
  std::string from = args.at(1).ToString();
  if (str.length() == 0) return (type::ValueFactory::GetVarcharValue(""));

  size_t pos = str.length() - 1;
  bool erase = 0;
  while (from.find(str[pos]) != std::string::npos) {
    erase = 1;
    pos--;
  }
  if (erase) str.erase(pos + 1, str.length() - pos - 1);

  pos = 0;
  erase = 0;
  while (from.find(str[pos]) != std::string::npos) {
    erase = 1;
    pos++;
  }
  if (erase) str.erase(0, pos);
  return (type::ValueFactory::GetVarcharValue(str));
}

}  // namespace expression
}  // namespace peloton