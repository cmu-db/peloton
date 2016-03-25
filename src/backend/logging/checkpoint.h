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
#include "backend/logging/log_manager.h"
#include "backend/logging/backend_logger.h"
#include "backend/common/pool.h"

namespace peloton {
namespace logging {

class Checkpoint {
  //===--------------------------------------------------------------------===//
  // Checkpoint
  //===--------------------------------------------------------------------===//

 public:
  Checkpoint() { pool.reset(new VarlenPool(BACKEND_TYPE_MM)); }

  virtual ~Checkpoint(void) {}
  virtual void DoCheckpoint() = 0;
  virtual void Init() = 0;
  virtual bool DoRecovery() = 0;

 protected:
  std::unique_ptr<VarlenPool> pool;
};

}  // namespace logging
}  // namespace peloton
