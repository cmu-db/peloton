//TODO include tile header file

namespace nstore {

bool AbstractPlanNode::init() {
    //TODO Insert any code common to all plan nodes here.
    p_init();
}

Tile AbstractPlanNode::getNextTile() {
    //TODO Insert any code common to all plan nodes here.
    return p_getNextTile();
}

void AbstractPlanNode::cleanup() {
    //TODO Insert any code common to all plan nodes here.
    p_cleanup();
}

} // namespace nstore
