#include "storage/tile.h"

#pragma once

namespace nstore {
namespace executor {

class AbstractExecutor {
  public:
    bool init();
    storage::Physical_Tile *getNextTile();
    void cleanup();
  protected:
    AbstractExecutor(AbstractPlanNode *abstract_node) {
        plan_node_ = abstract_node;
    }
    virtual bool p_init() = 0;
    virtual storage::Physical_Tile *p_getNextTile() = 0;
    virtual void p_cleanup() = 0;
  private:
    plannode::AbstractPlanNode plan_node_;
};

} // namespace executor
} // namespace nstore
