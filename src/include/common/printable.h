//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// printable.h
//
// Identification: src/include/common/printable.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iosfwd>
#include <string>

namespace peloton {

//===--------------------------------------------------------------------===//
// Printable Object
//===--------------------------------------------------------------------===//

class Printable {
 public:
  virtual ~Printable(){};

  /** @brief Get the info about the object. */
  virtual const std::string GetInfo() const = 0;

  // Get a string representation for debugging
  friend std::ostream &operator<<(std::ostream &os, const Printable &printable);
};

}  // namespace peloton
