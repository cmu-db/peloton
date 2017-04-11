//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// util.h
//
// Identification: src/include/optimizer/util.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdlib>
#include <algorithm>
#include <string>

namespace peloton {
namespace optimizer {
namespace util {

inline void to_lower_string(std::string &str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

} /* namespace util */
} /* namespace optimizer */
} /* namespace peloton */
