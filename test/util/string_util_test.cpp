//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// string_util_test.cpp
//
// Identification: test/common/string_util_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/string_util.h"
#include "common/harness.h"
#include "common/logger.h"

#include <string>
#include <utility>
#include <vector>

namespace peloton {
namespace test {

//===--------------------------------------------------------------------===//
// StringUtil Test
//===--------------------------------------------------------------------===//

class StringUtilTests : public PelotonTest {};

TEST_F(StringUtilTests, ContainsTest) {
  std::string str = "Word up, two for fives over here baby";
  EXPECT_TRUE(StringUtil::Contains(str, "fives"));
  EXPECT_TRUE(StringUtil::Contains(str, "two for fives"));
  EXPECT_FALSE(StringUtil::Contains(str, "CREAM"));
  EXPECT_TRUE(StringUtil::Contains(str, ""));
}

TEST_F(StringUtilTests, StartsWithTest) {
  std::string str = "I grew up on the crime side, the New York Times side";
  EXPECT_TRUE(StringUtil::StartsWith(str, "I"));
  EXPECT_TRUE(StringUtil::StartsWith(str, "I grew up"));
  EXPECT_TRUE(StringUtil::StartsWith(str, str));
  EXPECT_TRUE(StringUtil::StartsWith(str, ""));
  EXPECT_FALSE(StringUtil::StartsWith(str, "grew up"));
  EXPECT_FALSE(StringUtil::StartsWith(str, "CREAM"));
}

TEST_F(StringUtilTests, EndsWithTest) {
  std::string str = "Staying alive was no jive";
  EXPECT_TRUE(StringUtil::EndsWith(str, "jive"));
  EXPECT_TRUE(StringUtil::EndsWith(str, "no jive"));
  EXPECT_TRUE(StringUtil::EndsWith(str, str));
  EXPECT_TRUE(StringUtil::EndsWith(str, ""));
  EXPECT_FALSE(StringUtil::EndsWith(str, "Staying alive"));
  EXPECT_FALSE(StringUtil::EndsWith(str, "CREAM"));
}

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

    std::vector<std::string> lines = StringUtil::Split(result, '\n');
    for (std::string line : lines) {
      std::string substr = line.substr(0, prefix.size());
      EXPECT_EQ(prefix, substr);
    }
  }
}

TEST_F(StringUtilTests, FormatSizeTest) {
  std::vector<std::pair<long, std::string>> data = {
      std::make_pair(100, "100 bytes"), std::make_pair(1200, "1.17 KB"),
      std::make_pair(15721000, "14.99 MB"),
      std::make_pair(9990000000, "9.30 GB"),
  };

  for (auto x : data) {
    std::string result = StringUtil::FormatSize(x.first);
    EXPECT_FALSE(result.empty());
    LOG_TRACE("[%ld / '%s'] => %s", x.first, x.second.c_str(), result.c_str());
    EXPECT_EQ(x.second, result);
  }
}

TEST_F(StringUtilTests, UpperTest) {
  std::string input = "smoke crack rocks";
  std::string expected = "SMOKE CRACK ROCKS";
  std::string result = StringUtil::Upper(input);
  EXPECT_EQ(expected, result);
}

TEST_F(StringUtilTests, FormatIntTest) {
  int val = 10;

  // <FORMAT, EXPECTED>
  std::vector<std::pair<std::string, std::string>> data{
      std::make_pair("%5d", "   10"),   std::make_pair("%-5d", "10   "),
      std::make_pair("%05d", "00010"),  std::make_pair("%+5d", "  +10"),
      std::make_pair("%-+5d", "+10  "),
  };

  for (std::pair<std::string, std::string> x : data) {
    std::string result = StringUtil::Format(x.first, val);
    EXPECT_EQ(x.second, result);
  }
}

TEST_F(StringUtilTests, FormatFloatTest) {
  float val = 10.3456;

  // <FORMAT, EXPECTED>
  std::vector<std::pair<std::string, std::string>> data{
      std::make_pair("%.1f", "10.3"),
      std::make_pair("%.2f", "10.35"),
      std::make_pair("%8.2f", "   10.35"),
      std::make_pair("%8.4f", " 10.3456"),
      std::make_pair("%08.2f", "00010.35"),
      std::make_pair("%-8.2f", "10.35   "),
  };

  for (std::pair<std::string, std::string> x : data) {
    std::string result = StringUtil::Format(x.first, val);
    EXPECT_EQ(x.second, result);
  }
}

TEST_F(StringUtilTests, SplitTest) {
  // clang-format off
  std::vector<std::string> words = {
		  "Come", "on", "motherfuckers," "come", "on"};
  // clang-format on

  std::string splitChar = "_";
  for (int i = 1; i <= 5; i++) {
    std::string split = StringUtil::Repeat(splitChar, i);
    EXPECT_EQ(i, split.size());
    std::ostringstream os;
    for (auto w : words) {
      os << split << w;
    }
    os << split;
    std::string input = os.str();

    // Check that we can split both with the single char
    // and with the full string
    std::vector<std::string> result0 = StringUtil::Split(input, splitChar);
    EXPECT_EQ(words.size(), result0.size());
    EXPECT_EQ(words, result0);

    std::vector<std::string> result1 = StringUtil::Split(os.str(), split);
    EXPECT_EQ(words.size(), result1.size());
    EXPECT_EQ(words, result1);
  }  // FOR
}

}  // namespace test
}  // namespace peloton
