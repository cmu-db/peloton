#pragma once

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

//===--------------------------------------------------------------------===//
// Projection Executor
//===--------------------------------------------------------------------===//

class ProjectionExecutor : public AbstractExecutor {
  public:
    ProjectionExecutor(planner::AbstractPlanNode *abstract_node) :
        AbstractExecutor(abstract_node) {
    }

    bool Init();

    bool GetNextTile();

    void CleanUp();
};

} // namespace executor
} // namespace nstore
