//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// printable.cpp
//
// Identification: src/backend/common/printable.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "backend/common/printable.h"

namespace peloton {

// Get a string representation
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo() << std::endl;

  return os;
}

}  // End peloton namespace
