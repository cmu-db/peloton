#include "abstract_plan_node.h"

namespace nstore {
namespace executor {

bool AbstractPlanNode::init() {
    //TODO Insert any code common to all plan nodes here.
    p_init();
}

storage::Tile *AbstractPlanNode::getNextTile() {
    //TODO Insert any code common to all plan nodes here.
    return p_getNextTile();
}

void AbstractPlanNode::cleanup() {
    //TODO Insert any code common to all plan nodes here.
    p_cleanup();
}

} // namespace executor
} // namespace nstore
