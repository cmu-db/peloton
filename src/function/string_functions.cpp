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

#include <cctype>
#include <string>
#include <algorithm>

#include "function/string_functions.h"
#include "type/value_factory.h"

#define NextByte(p, plen) ((p)++, (plen)--)

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

bool StringFunctions::Like(const char *t, uint32_t tlen, const char *p,
                           uint32_t plen) {
  PL_ASSERT(t != nullptr);
  PL_ASSERT(p != nullptr);
  if (plen == 1 && *p == '%') return true;

  while (tlen > 0 && plen > 0) {
    if (*p == '\\') {
      NextByte(p, plen);
      if (plen <= 0) return false;
      if (tolower(*p) != tolower(*t)) return false;
    } else if (*p == '%') {
      char firstpat;
      NextByte(p, plen);

      while (plen > 0) {
        if (*p == '%')
          NextByte(p, plen);
        else if (*p == '_') {
          if (tlen <= 0) return false;
          NextByte(t, tlen);
          NextByte(p, plen);
        } else
          break;
      }

      if (plen <= 0) return true;

      if (*p == '\\') {
        if (plen < 2) return false;
        firstpat = tolower(p[1]);
      } else
        firstpat = tolower(*p);

      while (tlen > 0) {
        if (tolower(*t) == firstpat) {
          int matched = Like(t, tlen, p, plen);

          if (matched != false) return matched;
        }

        NextByte(t, tlen);
      }
      return false;
    } else if (*p == '_') {
      NextByte(t, tlen);
      NextByte(p, plen);
      continue;
    } else if (tolower(*p) != tolower(*t)) {
      return false;
    }
    NextByte(t, tlen);
    NextByte(p, plen);
  }

  if (tlen > 0) return false;

  while (plen > 0 && *p == '%') NextByte(p, plen);
  if (plen <= 0) return true;

  return false;
}

type::Value StringFunctions::_Like(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  bool ret = Like(args[0].GetAs<const char *>(), args[0].GetLength(),
                  args[1].GetAs<const char *>(), args[1].GetLength());
  return type::ValueFactory::GetBooleanValue(ret);
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
const char *StringFunctions::BTrim(const char *str, uint32_t str_len,
                                   const char *from,
                                   UNUSED_ATTRIBUTE uint32_t from_len,
                                   uint32_t *ret_len) {
  PL_ASSERT(str != nullptr && from != nullptr);
  if (str_len == 0) {
    *ret_len = 0;
    return str;
  }

  int tail = str_len - 1, head = 0;
  while (tail >= 0 && strchr(from, str[tail]) != NULL) tail--;

  while (head < (int)str_len && strchr(from, str[head]) != NULL) head++;

  *ret_len = std::max(tail - head + 1, 0);
  return str + head;
}

type::Value StringFunctions::_BTrim(const std::vector<type::Value> &args) {
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

uint32_t StringFunctions::Length(UNUSED_ATTRIBUTE const char *str,
                                 uint32_t length) {
  PL_ASSERT(str != nullptr);
  return length;
}

// The length of the string
type::Value StringFunctions::_Length(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  if (args[0].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  }

  uint32_t ret = Length(args[0].GetAs<const char *>(), args[0].GetLength());
  return type::ValueFactory::GetIntegerValue(ret);
}

}  // namespace function
}  // namespace peloton
