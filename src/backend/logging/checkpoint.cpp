/*-------------------------------------------------------------------------
 *
 * checkpoint.cpp
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "backend/logging/checkpoint.h"

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//
std::string Checkpoint::ConcatFileName(int version) {
  return FILE_PREFIX + std::to_string(version) + FILE_SUFFIX;
}

}  // namespace logging
}  // namespace peloton
