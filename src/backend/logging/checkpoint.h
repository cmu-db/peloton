/*-------------------------------------------------------------------------
 *
 * checkpoint.h
 * file description
 *
 * Copyright(c) 2015, CMU
 *
 * /peloton/src/backend/logging/checkpoint.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cassert>

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//

class Checkpoint {
 public:
  virtual ~Checkpoint(void){};
  virtual void DoCheckpoint() = 0;

 protected:
};

}  // namespace logging
}  // namespace peloton
