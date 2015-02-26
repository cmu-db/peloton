#pragma once

#include "executor/abstract_executor.h"

namespace nstore {
namespace executor {

class ProjectionExecutor : public AbstractExecutor {
  public:
    ProjectionExecutor(AbstractPlanNode *abstract_node) :
        AbstractExecutor(abstract_node) {};
  protected:
    bool p_init();
    bool p_getNextTile();
    void p_cleanup();
};

} // namespace executor
} // namespace nstore
