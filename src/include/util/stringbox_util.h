
//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stringbox_util.h
//
// Identification: /peloton/src/include/common/stringbox_util.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "util/string_util.h"

#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace peloton {

/**
 * Pretty-Print Box
 */
class StringBoxUtil {
 public:
  /**
   * Create a box around whatever you string you throw at it.
   * It will split the input based on newlines and pad the box
   * with a single space character all around.
   * It will automatically size the box based on the longest line in the input.
   */
  static std::string Box(const std::string &str) {
    std::string UNICODE_BOX_CORNERS[] = {"\u250C", "\u2510", "\u2514", "\u2518"};
    std::string UNICODE_BOX_VERTICAL = "\u2502";
    std::string UNICODE_BOX_HORIZONTAL = "\u2500";

    return StringBoxUtil::MakeBox(str, -1, UNICODE_BOX_HORIZONTAL, UNICODE_BOX_VERTICAL,
                                  UNICODE_BOX_CORNERS);
  }

  /**
   * Same has StringBoxUtil::Box except that it uses thicker lines.
   */
  static std::string HeavyBox(const std::string &str) {
    std::string UNICODE_HEAVYBOX_CORNERS[] = {"\u250F", "\u2513", "\u2517", "\u251B"};
    std::string UNICODE_HEAVYBOX_VERTICAL = "\u2503";
    std::string UNICODE_HEAVYBOX_HORIZONTAL = "\u2501";

    return StringBoxUtil::MakeBox(str, -1, UNICODE_HEAVYBOX_HORIZONTAL, UNICODE_HEAVYBOX_VERTICAL,
                                  UNICODE_HEAVYBOX_CORNERS);
  }

 private:
  static std::string MakeBox(const std::string &str, int max_len, std::string &horzMark,
                             std::string &vertMark, std::string corners[]);

};

}  // END peloton
