/*-------------------------------------------------------------------------
 *
 * kernel.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /n-store/src/backend/kernel.h
 *
 *-------------------------------------------------------------------------
 */

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
  static Result Handler(const char* query);
};

}  // namespace backend
}  // namespace peloton
