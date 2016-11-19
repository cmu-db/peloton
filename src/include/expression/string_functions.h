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

#include <string>
#include <vector>

#include "expression/abstract_expression.h"

namespace peloton {
namespace expression {

class StringFunctions{
public:
  // ASCII code of the first character of the argument.
  static common::Value Ascii(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    int32_t val = str[0];
    return ValueFactory::GetIntegerValue(val);
  }

  // Get Character from integer
  static common::Value Chr(const std::vector<common::Value>& args){
    int32_t val = args[0].GetAs<int32_t>();
    std::string str(1, char(static_cast<char>(val)));
    return ValueFactory::GetVarcharValue(str);
  }

  //substring
  static common::Value Substr(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    int32_t from = args[1].GetAs<int32_t>() - 1;
    int32_t len = args[2].GetAs<int32_t>();
    return ValueFactory::GetVarcharValue(str.substr(from, len));
  }

  // Number of characters in string
  static common::Value CharLength(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    int32_t len = str.length();
    return (ValueFactory::GetIntegerValue(len));
  }

  //Concatonate two strings
  static common::Value Concat(const std::vector<common::Value>& args){
    std::string str = args[0].ToString() + args[1].ToString();
    return (ValueFactory::GetVarcharValue(str));
  }

  // Number of bytes in string
  static common::Value OctetLength(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    int32_t len = str.length();
    return (ValueFactory::GetIntegerValue(len));
  }

  // Repeat string the specified number of times
  static common::Value Repeat(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    int32_t num = args[1].GetAs<int32_t>();
    std::string ret = "";
    while (num--) {
      ret = ret + str;
    }
    return (ValueFactory::GetVarcharValue(ret));
  }

  // Replace all occurrences in string of substring from with substring to
  static common::Value Replace(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    std::string from = args[1].ToString();
    std::string to = args[2].ToString();
    size_t pos = 0;
    while((pos = str.find(from, pos)) != std::string::npos) {
      str.replace(pos, from.length(), to);
      pos += to.length();
    }
    return (ValueFactory::GetVarcharValue(str));
  }

  // Remove the longest string containing only characters from characters
  // from the start of string
  static common::Value LTrim(const std::vector<common::Value>& args){
    std::string str = args[0].ToString();
    std::string from = args[1].ToString();
    size_t pos = 0;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos++;
    }
    if (erase)
      str.erase(0, pos);
    return (ValueFactory::GetVarcharValue(str));
  }

  // Remove the longest string containing only characters from characters
  // from the end of string
  static common::Value RTrim(const std::vector<common::Value>& args){
    std::string str = args.at(0).ToString();
    std::string from = args.at(1).ToString();
    if (str.length() == 0)
      return (ValueFactory::GetVarcharValue(""));
    size_t pos = str.length() - 1;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos--;
    }
    if (erase)
      str.erase(pos + 1, str.length() - pos - 1);
    return (ValueFactory::GetVarcharValue(str));
  }

  // Remove the longest string consisting only of characters in characters
  // from the start and end of string
  static common::Value BTrim(const std::vector<common::Value>& args){
    std::string str = args.at(0).ToString();
    std::string from = args.at(1).ToString();
    if (str.length() == 0)
      return (ValueFactory::GetVarcharValue(""));

    size_t pos = str.length() - 1;
    bool erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos--;
    }
    if (erase)
      str.erase(pos + 1, str.length() - pos - 1);

    pos = 0;
    erase = 0;
    while (from.find(str[pos]) != std::string::npos) {
      erase = 1;
      pos++;
    }
    if (erase)
      str.erase(0, pos);
    return (ValueFactory::GetVarcharValue(str));
  }
};
}  // namespace expression
}  // namespace peloton
