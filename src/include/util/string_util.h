//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// string_util.h
//
// Identification: /src/include/common/string_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

namespace peloton {

/**
 * This is the string that we should always use whenever we need
 * to do nice formatting in GetInfo()
 */
static const std::string GETINFO_SPACER = "  ";

static const std::string GETINFO_DOUBLE_STAR = "**";

static const std::string GETINFO_LONG_ARROW = "====>";

static const std::string GETINFO_SINGLE_LINE =
    "-------------------------------------------------------------";

static const std::string GETINFO_THICK_LINE =
    "=============================================================";

static const std::string GETINFO_HALF_THICK_LINE =
    "===========================";

static const int ARROW_INDENT = 3;

static const int TUPLE_ID_WIDTH = 6;

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
  static bool Contains(const std::string &haystack, const std::string &needle);

  /**
   * Returns true if the target string starts with the given prefix
   */
  static bool StartsWith(const std::string &str, const std::string &prefix);

  /**
   * Returns true if the target string <b>ends</b> with the given suffix.
   * http://stackoverflow.com/a/2072890
   */
  static bool EndsWith(const std::string &str, const std::string &suffix);

  /**
   * Repeat a string multiple times
   */
  static std::string Repeat(const std::string &str, const std::size_t n);

  /**
   * Split the input string based on newline char
   */
  static std::vector<std::string> Split(const std::string &str, char delimiter);

  /**
   * Append the prefix to the beginning of each line in str
   */
  static std::string Prefix(const std::string &str, const std::string &prefix);

  /**
   * Return a string that formats the give number of bytes into the appropriate
   * kilobyte, megabyte, gigabyte representation.
   * http://ubuntuforums.org/showpost.php?p=10215516&postcount=5
   */
  static std::string FormatSize(long bytes);

  /**
   * Wrap the given string with the control characters
   * to make the text appear bold in the console
   */
  static std::string Bold(const std::string &str);

  /**
   * Convert a string to its uppercase form
   */
  static std::string Upper(const std::string &str);

  /**
 * Convert a string to its uppercase form
 */
  static std::string Lower(const std::string &str);

  /**
   * Format a string using printf semantics
   * http://stackoverflow.com/a/8098080
   */
  static std::string Format(const std::string fmt_str, ...);

  /**
   * Split the input string into a vector of strings based on
   * the split string given us
   * @param input
   * @param split
   * @return
   */
  static std::vector<std::string> Split(const std::string &input,
                                        const std::string &split);

  /**
   * Remove the whitespace char in the right end of the string
   */
  static void RTrim(std::string &str);

  static std::string Indent(const int num_indent);
};

}  // namespace peloton
