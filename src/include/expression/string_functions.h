//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions.h
//
// Identification: src/include/expression/string_functions.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "type/value.h"

namespace peloton {
namespace expression {

class StringFunctions {
 public:
  // ASCII code of the first character of the argument.
  static type::Value Ascii(const std::vector<type::Value>& args);

  // Get Character from integer
  static type::Value Chr(const std::vector<type::Value>& args);

  // substring
  static type::Value Substr(const std::vector<type::Value>& args);

  // Number of characters in string
  static type::Value CharLength(const std::vector<type::Value>& args);

  // Concatenate two strings
  static type::Value Concat(const std::vector<type::Value>& args);

  // Number of bytes in string
  static type::Value OctetLength(const std::vector<type::Value>& args);

  // Repeat string the specified number of times
  static type::Value Repeat(const std::vector<type::Value>& args);

  // Replace all occurrences in string of substring from with substring to
  static type::Value Replace(const std::vector<type::Value>& args);

  // Remove the longest string containing only characters from characters
  // from the start of string
  static type::Value LTrim(const std::vector<type::Value>& args);

  // Remove the longest string containing only characters from characters
  // from the end of string
  static type::Value RTrim(const std::vector<type::Value>& args);

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static type::Value BTrim(const std::vector<type::Value>& args);
};
}  // namespace expression
}  // namespace peloton
