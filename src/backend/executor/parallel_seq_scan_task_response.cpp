//
// Created by Rui Wang on 16-4-5.
//

#include "parallel_seq_scan_task_response.h"

namespace peloton {
namespace executor {

ParallelSeqScanTaskResponse::ParallelSeqScanTaskResponse(
    ParallelTaskStatus status)
    : AbstractParallelTaskResponse(status) {}

ParallelSeqScanTaskResponse::ParallelSeqScanTaskResponse(
    ParallelTaskStatus status, LogicalTile *logicalTile)
    : AbstractParallelTaskResponse(status), logicalTile_(logicalTile) {}

LogicalTile *ParallelSeqScanTaskResponse::GetOutput() {
  return logicalTile_.release();
}

}  // executor
}  // peloton