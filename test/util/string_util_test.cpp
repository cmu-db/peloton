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

#include "util/string_util.h"
#include "common/harness.h"
#include "common/logger.h"

#include <string>
#include <vector>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// StringUtil Test
//===--------------------------------------------------------------------===//

class StringUtilTests : public PelotonTest {};

TEST_F(StringUtilTests, RepeatTest) {
  std::vector<int> sizes = {0, 1, 2, 4, 8, 16, 17};
  std::vector<std::string> strs = {"", "A", "XYZ"};

  for (int size : sizes) {
    for (std::string str : strs) {
      std::string result = StringUtil::Repeat(str, size);
      LOG_TRACE("[%d / '%s'] => '%s'", size, str.c_str(), result.c_str());

      if (size == 0 || str.size() == 0) {
        EXPECT_TRUE(result.empty());
      } else {
        EXPECT_FALSE(result.empty());
        EXPECT_EQ(size * str.size(), result.size());

        // Count to just double check...
        int occurrences = 0;
        std::string::size_type pos = 0;
        while ((pos = result.find(str, pos)) != std::string::npos) {
          occurrences++;
          pos += str.length();
        }
      }
    }
  }
}

TEST_F(StringUtilTests, PrefixTest) {
  std::string message =
      "My man Inf left a Tec and a nine at my crib\n"
      "Turned himself in, he had to do a bid\n"
      // Note the extra newline here
      "\n"
      "A one-to-three, he be home the end of '93\n"
      "I'm ready to get this paper, G, you with me?\n";

  std::vector<std::string> prefixes = {"*", ">>>"};

  for (std::string prefix : prefixes) {
    std::string result = StringUtil::Prefix(message, prefix);
    EXPECT_FALSE(result.empty());
    LOG_TRACE("[PREFIX=%s]\n%s\n=======\n", prefix.c_str(), result.c_str());

    std::vector<std::string> lines = StringUtil::Split(result);
    for (std::string line : lines) {
      std::string substr = line.substr(0, prefix.size());
      EXPECT_EQ(prefix, substr);
    }
  }
}

}  // End test namespace
}  // End peloton namespace
