
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// string_util.h
//
// Identification: /peloton/src/include/common/string_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace peloton {

/**
 * This is the string that we should always use whenever we need
 * to do nice formatting in GetInfo()
 */
static const std::string GETINFO_SPACER = "  ";

/**
 *
 */
static const std::string GETINFO_SINGLE_LINE =
    "-------------------------------------------------------------";

/**
 *
 */
static const std::string GETINFO_THICK_LINE =
    "=============================================================";

/**
 * String Utility Functions
 * Note that these are not the most efficient implementations (i.e., they copy
 * memory) and therefore they should only be used for debug messages and other
 * such things.
 */
class StringUtil {
 public:
  /**
   * Repeat a string multiple times
   */
  static std::string Repeat(const std::string &str, const std::size_t n) {
    std::stringstream os;
    if (n == 0 || str.empty()) {
      return (os.str());
    }
    for (int i = 0; i < static_cast<int>(n); i++) {
      os << str;
    }
    return (os.str());
  }

  /**
   * Split the input string based on newline char
   */
  static std::vector<std::string> Split(const std::string &str) {
    std::stringstream ss(str);
    std::vector<std::string> lines;
    std::string temp;
    while (std::getline(ss, temp, '\n')) {
      lines.push_back(temp);
    }  // WHILE
    return (lines);
  }

  /**
   * Append the prefix to the beginning of each line in str
   */
  static std::string Prefix(const std::string &str, const std::string &prefix) {
    std::vector<std::string> lines = StringUtil::Split(str);
    if (lines.empty()) return ("");

    std::stringstream os;
    for (int i = 0, cnt = lines.size(); i < cnt; i++) {
      if (i > 0) os << std::endl;
      os << prefix << lines[i];
    }  // FOR
    return (os.str());
  }
};

}  // END peloton
