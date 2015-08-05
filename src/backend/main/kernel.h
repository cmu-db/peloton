//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// kernel.h
//
// Identification: src/backend/main/kernel.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/types.h"

namespace peloton {
namespace backend {

//===--------------------------------------------------------------------===//
// Kernel
//===--------------------------------------------------------------------===//

// Main handler for query
class Kernel {
 public:
  static Result Handler(const char *query);
};

}  // namespace backend
}  // namespace peloton
