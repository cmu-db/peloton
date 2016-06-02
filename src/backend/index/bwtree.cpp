
#include "bwtree.h"

#ifdef BWTREE_PELOTON
namespace peloton {
namespace index {
#endif

// We use uint64_t(-1) as invalid node ID
NodeID INVALID_NODE_ID = NodeID(-1);

bool print_flag = true;

#ifdef BWTREE_PELOTON
}  // End index namespace
}  // End peloton namespace
#endif
