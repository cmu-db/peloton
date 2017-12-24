//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stringtable_util.h
//
// Identification: /src/include/common/stringtable_util.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "util/string_util.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <iomanip>

namespace peloton {
/**
 * Table Formatted String
 */
class StringTableUtil {
 public:

  static const int FIXED_WIDTH = 2;

  /**
   * Create a table to format the string.
   * Use '\n' to divide rows, '\t' to divide fields
   */
  static std::string Table(const std::string &str, bool header) {
    std::stringstream ss;
    std::vector<std::string> lines = StringUtil::Split(str, '\n');
    if (lines.size() == 0) return ("");

    std::vector<int> field_width;
    std::vector<std::vector<std::string>> table;

    for (std::string line : lines) {
      std::vector<std::string> tokens = StringUtil::Split(line,'\t');
      table.push_back(tokens);
      int i = 0;
      for (const std::string& token : tokens) {
        if (static_cast<int>(field_width.size()) < i + 1) {
          field_width.push_back(FIXED_WIDTH + static_cast<int>(token.length()));
        } else {
          field_width[i] = std::max(field_width[i], FIXED_WIDTH + static_cast<int>(token.length()));
        }
        i += 1;
      }
    }

    int max_length = 0;
    for (int i = 0; i < static_cast<int>(field_width.size()); i++) {
      max_length += field_width[i];
    }

    int row_num = static_cast<int>(table.size());
    int i = 0;
    for (const std::vector<std::string>& row : table) {
      for (int j = 0; j < static_cast<int>(row.size()); j++) {
        ss << std::setw(field_width.at(j)) << row.at(j);
      }
      if (header && i == 0) {
        ss << std::endl << StringUtil::Repeat("-", max_length) << std::endl;
      } else if (i != row_num - 1) {
        ss << std::endl;
      }
      i += 1;
    }
    return ss.str();
  }
};

}  // namespace peloton
