//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.h
//
// Identification: src/include/function/string_functions.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "type/value.h"

namespace peloton {
namespace function {

class StringFunctions {
 public:
  struct StrWithLen {
    StrWithLen(const char *str, uint32_t length) : str(str), length(length) {}
    const char *str;
    uint32_t length;
  };

  // ASCII code of the first character of the argument.
  static uint32_t Ascii(const char *str, uint32_t length);
  static type::Value _Ascii(const std::vector<type::Value> &args);

  // Like
  static bool Like(const char *t, uint32_t tlen, const char *p, uint32_t plen);
  static type::Value _Like(const std::vector<type::Value> &args);

  // Get Character from integer
  static type::Value Chr(const std::vector<type::Value> &args);

  // substring
  static StrWithLen Substr(const char *str, uint32_t str_length, int32_t from,
                           int32_t len);
  static type::Value _Substr(const std::vector<type::Value> &args);

  // Number of characters in string
  static type::Value CharLength(const std::vector<type::Value> &args);

  // Concatenate two strings
  static type::Value Concat(const std::vector<type::Value> &args);

  // Number of bytes in string
  static type::Value OctetLength(const std::vector<type::Value> &args);

  // Repeat string the specified number of times
  static type::Value Repeat(const std::vector<type::Value> &args);

  // Replace all occurrences in string of substring from with substring to
  static type::Value Replace(const std::vector<type::Value> &args);

  // Remove the longest string containing only characters from characters
  // from the start of string
  static StrWithLen LTrim(const char *str, uint32_t str_len, const char *from,
                          uint32_t from_len);
  static type::Value _LTrim(const std::vector<type::Value> &args);

  // Remove the longest string containing only characters from characters
  // from the end of string
  static StrWithLen RTrim(const char *str, uint32_t str_len, const char *from,
                          uint32_t from_len);
  static type::Value _RTrim(const std::vector<type::Value> &args);

  static StrWithLen Trim(const char *str, uint32_t str_len);
  static type::Value _Trim(const std::vector<type::Value> &args);

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static StrWithLen BTrim(const char *str, uint32_t str_len, const char *from,
                          uint32_t from_len);
  static type::Value _BTrim(const std::vector<type::Value> &args);

  // This function is used by LLVM engine
  // Length will return the number of characters in the given string
  static uint32_t Length(const char *str, uint32_t length);
  static type::Value _Length(const std::vector<type::Value> &args);
};

}  // namespace function
}  // namespace peloton
