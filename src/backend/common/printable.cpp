//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// printable.cpp
//
// Identification: src/backend/common/printable.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "backend/common/printable.h"

namespace peloton {

// Get a string representation for debugging
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo() << "\n";
  return os;
};

}  // namespace peloton
