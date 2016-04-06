//
// Created by Rui Wang on 16-4-4.
//

#include "abstract_parallel_task_response.h"

namespace peloton {
namespace executor  {

AbstractParallelTaskResponse::AbstractParallelTaskResponse(): status_(Unknown) { }

AbstractParallelTaskResponse::AbstractParallelTaskResponse(ParallelTaskStatus status): status_(status) { }


ParallelTaskStatus AbstractParallelTaskResponse::GetStatus() {
  return status_;
}
}  // namespace executor
}  // namespace peloton