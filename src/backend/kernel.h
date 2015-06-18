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

#include "common/types.h"

namespace nstore {
namespace backend {

//===--------------------------------------------------------------------===//
// Kernel
//===--------------------------------------------------------------------===//

// Main handler for query
class Kernel {

 public:

  static ResultType Handler(const char* query);

};

} // namespace backend
} // namespace nstore
