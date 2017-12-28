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

#include <algorithm>
#include <cctype>
#include <string>

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

StringFunctions::StrWithLen StringFunctions::Substr(const char *str,
                                                    uint32_t str_length,
                                                    int32_t from, int32_t len) {
  int32_t signed_end = from + len - 1;
  if (signed_end < 0 || str_length == 0)
    return StringFunctions::StrWithLen(nullptr, 0);

  uint32_t begin = from > 0 ? unsigned(from) - 1 : 0;
  uint32_t end = unsigned(signed_end);
  if (end > str_length) end = str_length;
  if (begin > end) return StringFunctions::StrWithLen(nullptr, 0);
  return StringFunctions::StrWithLen(str + begin, end - begin + 1);
}

// substring
type::Value StringFunctions::_Substr(const std::vector<type::Value> &args) {
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

StringFunctions::StrWithLen StringFunctions::LTrim(const char *str,
                                                   uint32_t str_len,
                                                   const char *from,
                                                   UNUSED_ATTRIBUTE uint32_t
                                                       from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);
  // llvm expects the len to include the terminating '\0'
  if (str_len == 1) {
    return StringFunctions::StrWithLen(nullptr, 1);
  }

  str_len -= 1;
  int tail = str_len - 1, head = 0;

  while (head < (int)str_len && strchr(from, str[head]) != NULL) head++;

  return StringFunctions::StrWithLen(str + head,
                                     std::max(tail - head + 1, 0) + 1);
}

// Remove the longest string containing only characters from characters
// from the start of string
type::Value StringFunctions::_LTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  StrWithLen ret =
      LTrim(args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
            args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
}

StringFunctions::StrWithLen StringFunctions::RTrim(const char *str,
                                                   uint32_t str_len,
                                                   const char *from,
                                                   UNUSED_ATTRIBUTE uint32_t
                                                       from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);
  // llvm expects the len to include the terminating '\0'
  if (str_len == 1) {
    return StringFunctions::StrWithLen(nullptr, 1);
  }

  str_len -= 1;
  int tail = str_len - 1, head = 0;
  while (tail >= 0 && strchr(from, str[tail]) != NULL) tail--;

  return StringFunctions::StrWithLen(str + head,
                                     std::max(tail - head + 1, 0) + 1);
}

// Remove the longest string containing only characters from characters
// from the end of string
type::Value StringFunctions::_RTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  StrWithLen ret =
      RTrim(args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
            args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
}

StringFunctions::StrWithLen StringFunctions::Trim(const char *str,
                                                  uint32_t str_len) {
  return BTrim(str, str_len, " ", 2);
}

type::Value StringFunctions::_Trim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 1);
  return _BTrim({args[0], type::ValueFactory::GetVarcharValue(" ")});
}

// Remove the longest string consisting only of characters in characters
// from the start and end of string
StringFunctions::StrWithLen StringFunctions::BTrim(const char *str,
                                                   uint32_t str_len,
                                                   const char *from,
                                                   UNUSED_ATTRIBUTE uint32_t
                                                       from_len) {
  PL_ASSERT(str != nullptr && from != nullptr);

  str_len--;  // skip the tailing 0

  if (str_len == 0) {
    return StringFunctions::StrWithLen(str, 1);
  }

  int tail = str_len - 1, head = 0;
  while (tail >= 0 && strchr(from, str[tail]) != NULL) tail--;

  while (head < (int)str_len && strchr(from, str[head]) != NULL) head++;

  return StringFunctions::StrWithLen(str + head,
                                     std::max(tail - head + 1, 0) + 1);
}

type::Value StringFunctions::_BTrim(const std::vector<type::Value> &args) {
  PL_ASSERT(args.size() == 2);
  if (args[0].IsNull() || args[1].IsNull()) {
    return type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
  }

  StrWithLen ret =
      BTrim(args.at(0).GetData(), strlen(args.at(0).GetData()) + 1,
            args.at(1).GetData(), strlen(args.at(1).GetData()) + 1);

  std::string str(ret.str, ret.length - 1);
  return type::ValueFactory::GetVarcharValue(str);
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
