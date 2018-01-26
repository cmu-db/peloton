//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// printable.cpp
//
// Identification: src/common/printable.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/printable.h"

namespace peloton {

// Get a string representation for debugging
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo();
  return os;
};

}  // namespace peloton
