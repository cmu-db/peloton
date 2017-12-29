//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_functions_test.cpp
//
// Identification: test/expression/string_functions_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <set>
#include <string>
#include <vector>

#include "common/harness.h"

#include "function/string_functions.h"
#include "common/internal_types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "util/string_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class StringFunctionsTests : public PelotonTest {};

TEST_F(StringFunctionsTests, LikeTest) {
  //-------------- match ---------------- //
  std::string s1 = "forbes \\avenue";  // "forbes \avenue"
  std::string p1 = "%b_s \\\\avenue";  // "%b_s \\avenue"
  EXPECT_TRUE(function::StringFunctions::Like(s1.c_str(), s1.size(), p1.c_str(),
                                              p1.size()));

  std::string s2 = "for%bes avenue%";    // "for%bes avenue%"
  std::string p2 = "for%bes a_enue\\%";  // "for%bes a_enue%"
  EXPECT_TRUE(function::StringFunctions::Like(s2.c_str(), s2.size(), p2.c_str(),
                                              p2.size()));

  std::string s3 = "Allison";  // "Allison"
  std::string p3 = "%lison";   // "%lison"
  EXPECT_TRUE(function::StringFunctions::Like(s3.c_str(), s3.size(), p3.c_str(),
                                              p3.size()));

  //----------Exact Match------------//
  std::string s5 = "Allison";  // "Allison"
  std::string p5 = "Allison";  // "Allison"
  EXPECT_TRUE(function::StringFunctions::Like(s5.c_str(), s5.size(), p5.c_str(),
                                              p5.size()));

  //----------Exact Match------------//
  std::string s6 = "Allison";   // "Allison"
  std::string p6 = "A%llison";  // "A%llison"
  EXPECT_TRUE(function::StringFunctions::Like(s6.c_str(), s6.size(), p6.c_str(),
                                              p6.size()));

  //-------------- not match ----------------//
  std::string s4 = "forbes avenue";  // "forbes avenue"
  std::string p4 = "f_bes avenue";   // "f_bes avenue"
  EXPECT_FALSE(function::StringFunctions::Like(s4.c_str(), s4.size(),
                                               p4.c_str(), p4.size()));
}

TEST_F(StringFunctionsTests, AsciiTest) {
  const char column_char = 'A';
  for (int i = 0; i < 52; i++) {
    int expected = (int)column_char + i;

    std::ostringstream os;
    os << static_cast<char>(expected);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(os.str())};

    auto result = function::StringFunctions::_Ascii(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::StringFunctions::_Ascii(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, ChrTest) {
  const char column_char = 'A';
  for (int i = 0; i < 52; i++) {
    int char_int = (int)column_char + i;

    std::ostringstream os;
    os << static_cast<char>(char_int);
    std::string expected = os.str();

    std::vector<type::Value> args = {
        type::ValueFactory::GetIntegerValue(char_int)};

    auto result = function::StringFunctions::Chr(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER)};
  auto result = function::StringFunctions::Chr(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, SubstrTest) {
  std::vector<std::string> words = {"Fuck", "yo", "couch"};
  std::ostringstream os;
  for (auto w : words) {
    os << w;
  }
  int from = words[0].size() + 1;
  int len = words[1].size();
  const std::string expected = words[1];
  std::vector<type::Value> args = {
      type::ValueFactory::GetVarcharValue(os.str()),
      type::ValueFactory::GetIntegerValue(from),
      type::ValueFactory::GetIntegerValue(len),
  };
  auto result = function::StringFunctions::_Substr(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 3; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
        type::ValueFactory::GetVarcharValue("ccc"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::StringFunctions::_Substr(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, CharLengthTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string input = StringUtil::Repeat(str, i);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(input)};

    auto result = function::StringFunctions::CharLength(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(i, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::StringFunctions::CharLength(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, RepeatTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string expected = StringUtil::Repeat(str, i);
    EXPECT_EQ(i, expected.size());
    std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(str),
                                     type::ValueFactory::GetIntegerValue(i)};

    auto result = function::StringFunctions::Repeat(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR),
      type::ValueFactory::GetVarcharValue(str),
  };
  auto result = function::StringFunctions::Repeat(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, ReplaceTest) {
  const std::string origChar = "A";
  const std::string replaceChar = "X";
  const std::string prefix = "**PAVLO**";
  for (int i = 0; i < 100; i++) {
    std::string expected = prefix + StringUtil::Repeat(origChar, i);
    EXPECT_EQ(i + prefix.size(), expected.size());
    std::string input = prefix + StringUtil::Repeat(replaceChar, i);
    EXPECT_EQ(i + prefix.size(), expected.size());

    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(input),
        type::ValueFactory::GetVarcharValue(replaceChar),
        type::ValueFactory::GetVarcharValue(origChar)};

    auto result = function::StringFunctions::Replace(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 3; i++) {
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
        type::ValueFactory::GetVarcharValue("ccc"),
    };
    args[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::StringFunctions::Replace(args);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, LTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = message + spaces;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::StringFunctions::_LTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::StringFunctions::_LTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, RTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = spaces + message;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::StringFunctions::_RTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::StringFunctions::_RTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, BTrimTest) {
  const std::string message = "This is a string with spaces";
  const std::string spaces = "    ";
  const std::string origStr = spaces + message + spaces;
  const std::string expected = message;
  std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(origStr),
                                   type::ValueFactory::GetVarcharValue(" ")};
  auto result = function::StringFunctions::_BTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  result = function::StringFunctions::_Trim(
      {type::ValueFactory::GetVarcharValue(origStr)});
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = function::StringFunctions::_BTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, LengthTest) {
  const char column_char = 'A';
  int expected = 1;
  std::string str = "";
  for (int i = 0; i < 52; i++) {
    str += (char)(column_char + i);
    expected++;

    std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(str)};

    auto result = function::StringFunctions::_Length(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = function::StringFunctions::_Length(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, CodegenSubstrTest) {
  const std::string message = "1234567";
  int from = 1;
  int len = 5;
  std::string expected = message.substr(from - 1, len);
  auto res = function::StringFunctions::Substr(message.c_str(),
                                               message.length(), from, len);
  EXPECT_EQ(len + 1, res.length);
  EXPECT_EQ(expected, std::string(res.str, len));

  from = 7;
  len = 1;
  expected = message.substr(from - 1, len);
  res = function::StringFunctions::Substr(message.c_str(), message.length(),
                                          from, len);
  EXPECT_EQ(len + 1, res.length);
  EXPECT_EQ(expected, std::string(res.str, len));

  from = -2;
  len = 4;
  expected = message.substr(0, 1);
  res = function::StringFunctions::Substr(message.c_str(), message.length(),
                                          from, len);
  EXPECT_EQ(2, res.length);
  EXPECT_EQ(expected, std::string(res.str, 1));

  from = -2;
  len = 2;
  expected = "";
  res = function::StringFunctions::Substr(message.c_str(), message.length(),
                                          from, len);
  EXPECT_EQ(0, res.length);
  EXPECT_EQ(nullptr, res.str);
}

}  // namespace test
}  // namespace peloton
