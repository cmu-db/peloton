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

#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "include/function/string_functions.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"
#include "util/string_util.h"

using ::testing::NotNull;
using ::testing::Return;

namespace peloton {
namespace test {

class StringFunctionsTests : public PelotonTest {};

TEST_F(StringFunctionsTests, AsciiTest) {
  const char column_char = 'A';
  for (int i = 0; i < 52; i++) {
    int expected = (int)column_char + i;

    std::ostringstream os;
    os << static_cast<char>(expected);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(os.str())};

    auto result = expression::StringFunctions::Ascii(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = expression::StringFunctions::Ascii(args);
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

    auto result = expression::StringFunctions::Chr(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER)};
  auto result = expression::StringFunctions::Chr(args);
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
  auto result = expression::StringFunctions::Substr(args);
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
    auto result = expression::StringFunctions::Substr(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

TEST_F(StringFunctionsTests, CharLengthTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string input = StringUtil::Repeat(str, i);
    std::vector<type::Value> args = {
        type::ValueFactory::GetVarcharValue(input)};

    auto result = expression::StringFunctions::CharLength(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(i, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR)};
  auto result = expression::StringFunctions::CharLength(args);
  EXPECT_TRUE(result.IsNull());
}

TEST_F(StringFunctionsTests, RepeatTest) {
  const std::string str = "A";
  for (int i = 0; i < 100; i++) {
    std::string expected = StringUtil::Repeat(str, i);
    EXPECT_EQ(i, expected.size());
    std::vector<type::Value> args = {type::ValueFactory::GetVarcharValue(str),
                                     type::ValueFactory::GetIntegerValue(i)};

    auto result = expression::StringFunctions::Repeat(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR),
      type::ValueFactory::GetVarcharValue(str),
  };
  auto result = expression::StringFunctions::Repeat(args);
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

    auto result = expression::StringFunctions::Replace(args);
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
    auto result = expression::StringFunctions::Replace(args);
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
  auto result = expression::StringFunctions::LTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = expression::StringFunctions::LTrim(nullargs);
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
  auto result = expression::StringFunctions::RTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = expression::StringFunctions::RTrim(nullargs);
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
  auto result = expression::StringFunctions::BTrim(args);
  EXPECT_FALSE(result.IsNull());
  EXPECT_EQ(expected, result.ToString());

  // Use NULL for every argument and make sure that it always returns NULL.
  for (int i = 0; i < 2; i++) {
    std::vector<type::Value> nullargs = {
        type::ValueFactory::GetVarcharValue("aaa"),
        type::ValueFactory::GetVarcharValue("bbb"),
    };
    nullargs[i] = type::ValueFactory::GetNullValueByType(type::TypeId::VARCHAR);
    auto result = expression::StringFunctions::BTrim(nullargs);
    EXPECT_TRUE(result.IsNull());
  }
}

}  // namespace test
}  // namespace peloton
