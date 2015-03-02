#include "abstract_executor.h"

#include "executor/abstract_plan_node.h"

namespace nstore {
namespace executor {

bool AbstractExecutor::init() {
    //TODO Insert any code common to all executors here.
    p_init();
}

storage::Tile *AbstractExecutor::getNextTile() {
    //TODO Insert any code common to all executors here.
    return p_getNextTile();
}

void AbstractExecutor::cleanup() {
    //TODO Insert any code common to all executors here.
    p_cleanup();
}

} // namespace executor
} // namespace nstore
