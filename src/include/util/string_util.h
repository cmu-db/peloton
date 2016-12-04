
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
 *
 */
static const int TUPLE_ID_WIDTH = 6;

/**
 *
 */
static const int TXN_ID_WIDTH = 10;

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
    std::ostringstream os;
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

    std::ostringstream os;
    for (int i = 0, cnt = lines.size(); i < cnt; i++) {
      if (i > 0) os << std::endl;
      os << prefix << lines[i];
    }  // FOR
    return (os.str());
  }

  /**
   * Return a string that formats the give number of bytes into the appropriate
   * kilobyte, megabyte, gigabyte representation.
   * http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
   */
  static std::string FormatSize(long bytes) {
    double BASE = 1024;
    double KB = BASE;
    double MB = KB * BASE;
    double GB = MB * BASE;

    std::ostringstream os;

    if (bytes >= GB) {
      os << std::fixed << std::setprecision(2) << (bytes / GB) << " GB";
    } else if (bytes >= MB) {
      os << std::fixed << std::setprecision(2) << (bytes / MB) << " MB";
    } else if (bytes >= KB) {
      os << std::fixed << std::setprecision(2) << (bytes / KB) << " KB";
    } else {
      os << std::to_string(bytes) + " bytes";
    }
    return (os.str());
  }

  /**
   * Wrap the given string with the control characters
   * to make the text appear bold in the console
   */
  static std::string Bold(const std::string &str) {
    std::string SET_PLAIN_TEXT = "\033[0;0m";
    std::string SET_BOLD_TEXT = "\033[0;1m";

    std::ostringstream os;
    os << SET_BOLD_TEXT << str << SET_PLAIN_TEXT;
    return (os.str());
  }
};

}  // END peloton
