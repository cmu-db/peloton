//
// Created by Rui Wang on 16-4-4.
//

#pragma once

#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

enum ParallelTaskStatus {
  Unknown,
  HasRetValue,
  NoRetValue,
  Abort
};

class AbstractParallelTaskResponse {
public:
  AbstractParallelTaskResponse();
  AbstractParallelTaskResponse(ParallelTaskStatus status);

  ParallelTaskStatus GetStatus();

  virtual LogicalTile *GetOutput() = 0;
private:
  ParallelTaskStatus  status_;
};


}  // namespace executor
}  // namespace peloton
