//
// Created by Rui Wang on 16-4-5.
//

#pragma once

#include "backend/executor/abstract_parallel_task_response.h"
#include "backend/executor/logical_tile.h"

namespace peloton {
namespace executor {

class ParallelSeqScanTaskResponse : public AbstractParallelTaskResponse {
 public:
  ParallelSeqScanTaskResponse(ParallelTaskStatus status);

  ParallelSeqScanTaskResponse(ParallelTaskStatus status,
                              LogicalTile *logicalTile);

  LogicalTile *GetOutput();

 private:
  std::unique_ptr<LogicalTile> logicalTile_;
};

}  // executor
}  // peloton