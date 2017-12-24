//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stringtable_util_test.cpp
//
// Identification: test/util/stringtable_util_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/stringtable_util.h"
#include "common/harness.h"

#include <vector>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// StringTableUtil Test
//===--------------------------------------------------------------------===//

class StringTableUtilTests : public PelotonTest {};

void CheckTable(std::string &table, bool header, int num_lines) {
  // Make sure that it has the same number of lines as the input
  // plus 1 extra line if containing header
  std::vector<std::string> lines = StringUtil::Split(table, '\n');
  if (header) {
    EXPECT_EQ(num_lines + 1, static_cast<int>(lines.size()));
  } else {
    EXPECT_EQ(num_lines, static_cast<int>(lines.size()));
  }
}

TEST_F(StringTableUtilTests, BoxTest) {
  std::string message =
      "Meeting\tRoom\tPeople\n"
          "Peloton\t9001\tA\n"
          "Bike\t8001\tB, C, D\n"
          "Transformer\t7001\tE, F, G\n";
  std::string result = StringTableUtil::Table(message, true);
  EXPECT_TRUE(result.size() > 0);
  LOG_INFO("\n%s", result.c_str());
  CheckTable(result, true, 4);

  message =
      "Halloween\tOctorber\n"
          "Thanksgiving\tNovember\n"
          "Christmas\tDecember\n";
  result = StringTableUtil::Table(message, false);
  LOG_INFO("\n%s", result.c_str());
  CheckTable(result, false, 3);
}

}  // namespace test
}  // namespace peloton
