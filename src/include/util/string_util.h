
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

#include <stdarg.h>
#include <string.h>
#include <algorithm>
#include <iomanip>
#include <memory>
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
   * Returns true if the needle string exists in the haystack
   */
  static inline bool Contains(const std::string &haystack,
                              const std::string &needle) {
    return (haystack.find(needle) != std::string::npos);
  }

  /**
   * Returns true if the target string starts with the given prefix
   */
  static inline bool StartsWith(const std::string &str,
                                const std::string &prefix) {
    return std::equal(prefix.begin(), prefix.end(), str.begin());
  }

  /**
   * Returns true if the target string <b>ends</b> with the given suffix.
   * http://stackoverflow.com/a/2072890
   */
  static inline bool EndsWith(const std::string &str,
                              const std::string &suffix) {
    if (suffix.size() > str.size()) return (false);
    return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
  }

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

  /**
   * Convert a string to its uppercase form
   */
  static std::string Upper(const std::string &str) {
    std::string copy(str);
    std::transform(copy.begin(), copy.end(), copy.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return (copy);
  }

  /**
   * Format a string using printf semantics
   * http://stackoverflow.com/a/8098080
   */
  static std::string Format(const std::string fmt_str, ...) {
    // Reserve two times as much as the length of the fmt_str
    int final_n, n = ((int)fmt_str.size()) * 2;
    std::string str;
    std::unique_ptr<char[]> formatted;
    va_list ap;

    while (1) {
      // Wrap the plain char array into the unique_ptr
      formatted.reset(new char[n]);
      strcpy(&formatted[0], fmt_str.c_str());
      va_start(ap, fmt_str);
      final_n = vsnprintf(&formatted[0], n, fmt_str.c_str(), ap);
      va_end(ap);
      if (final_n < 0 || final_n >= n)
        n += abs(final_n - n + 1);
      else
        break;
    }
    return std::string(formatted.get());
  }
};

}  // END peloton
