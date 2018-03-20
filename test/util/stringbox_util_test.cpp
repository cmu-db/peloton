//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// boolean_value_test.cpp
//
// Identification: test/common/boolean_value_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/stringbox_util.h"
#include "common/harness.h"

#include <vector>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// StringBoxUtil Test
//===--------------------------------------------------------------------===//

class StringBoxUtilTests : public PelotonTest {};

void CheckBox(std::string &box, std::string key, int num_lines) {
  // Make sure that our search key is in the box
  EXPECT_NE(std::string::npos, box.find(key));

  // Make sure that it has the same number of lines as the input
  // plus two extra ones for the border
  std::vector<std::string> lines = StringUtil::Split(box, '\n');
  EXPECT_EQ(num_lines + 2, static_cast<int>(lines.size()));
}

TEST_F(StringBoxUtilTests, BoxTest) {
  std::string message =
      "Drunk as hell but no throwing up\n"
      "Half way home and my pager still blowing up\n"
      "Today I didn't even have to use my A.K.\n"
      "I got to say it was a good day";

  std::string result = StringBoxUtil::Box(message);
  EXPECT_TRUE(result.size() > 0);
  CheckBox(result, "throwing up", 4);

  result = StringBoxUtil::HeavyBox(message);
  EXPECT_TRUE(result.size() > 0);
  CheckBox(result, "Today I didn't", 4);
}

}  // namespace test
}  // namespace peloton
