
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
 * String Utility Functions
 */
class StringUtil {
 public:
  /**
   * Repeat a string multiple times
   * http://stackoverflow.com/a/34321702
   */
  static std::string repeat(std::string str, const std::size_t n) {
    if (n == 0) {
      str.clear();
      str.shrink_to_fit();
      return str;
    }

    if (n == 1 || str.empty()) return str;

    const auto period = str.size();

    if (period == 1) {
      str.append(n - 1, str.front());
      return str;
    }

    str.reserve(period * n);

    std::size_t m{2};

    for (; m < n; m *= 2) str += str;

    str.append(str.c_str(), (n - (m / 2)) * period);

    return str;
  }

  static std::vector<std::string> split(const std::string str) {
    std::stringstream ss(str);
    std::vector<std::string> lines;
    std::string temp;
    while (std::getline(ss, temp, '\n')) {
      lines.push_back(temp);
    }  // WHILE
    return (lines);
  }
};

}  // END peloton
