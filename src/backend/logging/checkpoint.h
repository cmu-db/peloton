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

namespace peloton {
namespace logging {

//===--------------------------------------------------------------------===//
// Checkpoint
//===--------------------------------------------------------------------===//

class Checkpoint {
 public:
  virtual ~Checkpoint(void){};

 protected:
};

}  // namespace logging
}  // namespace peloton
