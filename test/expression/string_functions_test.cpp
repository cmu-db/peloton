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

#include "expression/string_functions.h"
#include "expression/expression_util.h"
#include "expression/function_expression.h"
#include "type/types.h"
#include "type/value.h"
#include "type/value_factory.h"

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
      type::ValueFactory::GetVarcharValue(os.str())
    };

    auto result = expression::StringFunctions::Ascii(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.GetAs<int>());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::Type::VARCHAR)
  };
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
        type::ValueFactory::GetIntegerValue(char_int)
    };

    auto result = expression::StringFunctions::Chr(args);
    EXPECT_FALSE(result.IsNull());
    EXPECT_EQ(expected, result.ToString());
  }
  // NULL CHECK
  std::vector<type::Value> args = {
      type::ValueFactory::GetNullValueByType(type::Type::INTEGER)
  };
  auto result = expression::StringFunctions::Chr(args);
  EXPECT_TRUE(result.IsNull());
}

}  // namespace test
}  // namespace peloton
