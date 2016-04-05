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
std::string Checkpoint::ConcatFileName(std::string checkpoint_dir,
                                       int version) {
  return checkpoint_dir + "/" + FILE_PREFIX + std::to_string(version) +
         FILE_SUFFIX;
}

void Checkpoint::InitDirectory() {
  int return_val;

  return_val = mkdir(checkpoint_dir.c_str(), 0700);
  LOG_INFO("Checkpoint directory is: %s", checkpoint_dir.c_str());

  if (return_val == 0) {
    LOG_INFO("Created checkpoint directory successfully");
  } else if (return_val == EEXIST) {
    LOG_INFO("Checkpoint Directory already exists");
  } else {
    LOG_ERROR("Creating checkpoint directory failed: %s", strerror(errno));
  }
}

}  // namespace logging
}  // namespace peloton
