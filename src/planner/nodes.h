/**
 * @brief Header for all plan nodes
 *
 * Copyright(c) 2015, CMU
 *
 */


#pragma once

namespace nstore {
namespace planner {

// This is just for convenience

#include "planner/aggregate_node.h"
#include "planner/delete_node.h"
#include "planner/distinct_node.h"
#include "planner/indexscan_node.h"
#include "planner/insert_node.h"
#include "planner/limit_node.h"
#include "planner/materialize_node.h"
#include "planner/nestloop_node.h"
#include "planner/nestloopindex_node.h"
#include "planner/projection_node.h"
#include "planner/orderby_node.h"
#include "planner/receive_node.h"
#include "planner/send_node.h"
#include "planner/seqscan_node.h"
#include "planner/union_node.h"
#include "planner/update_node.h"

} // namespace planner
} // namespace nstore

