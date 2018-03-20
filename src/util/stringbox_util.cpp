//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stringbox_util.cpp
//
// Identification: /peloton/src/util/stringbox_util.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "util/stringbox_util.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace peloton {

std::string StringBoxUtil::MakeBox(const std::string &str, int max_len, std::string &horzMark,
                           std::string &vertMark, std::string corners[]) {
  std::vector<std::string> lines = StringUtil::Split(str, '\n');
  if (lines.size() == 0) return ("");

  // CORNERS:
  //  0: Top-Left
  //  1: Top-Right
  //  2: Bottom-Left
  //  3: Bottom-Right

  if (max_len <= 0) {
    for (std::string line : lines) {
      int line_len = static_cast<int>(line.length());
      if (max_len <= 0 || line_len > max_len) {
        max_len = line_len;
      }
    }  // FOR
  }

  std::stringstream os;

  // TOP LINE :: padding - two corners
  os << corners[0];
  os << StringUtil::Repeat(horzMark, max_len + 2);
  os << corners[1];
  os << std::endl;

  // INNER
  for (std::string line : lines) {
    os << vertMark << " ";
    os << std::left << std::setw(max_len) << line;
    os << " " << vertMark << std::endl;
  }  // FOR

  // BOTTOM LINE :: padding - two corners
  os << corners[2];
  os << StringUtil::Repeat(horzMark, max_len + 2);
  os << corners[3];

  return (os.str());
}


}