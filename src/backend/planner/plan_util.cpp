//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// plan_node_util.cpp
//
// Identification: src/backend/planner/plan_node_util.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "plan_util.h"

#include <sstream>

#include "backend/common/types.h"
#include "backend/common/exception.h"
#include "backend/common/logger.h"

namespace peloton {
namespace planner {

AbstractPlan *GetEmptyPlanNode(PlanNodeType type) {
  // TODO Uncomment after fixing compilation error.
  // LOG4CXX_TRACE(logger, "Creating an empty PlanNode of type : " <<
  // PlanNodeTypeToString(type));
  AbstractPlan *ret = NULL;

  switch (type) {
    /*
    case (PLAN_NODE_TYPE_SEQSCAN):
        ret = new SeqScanPlanNode();
    break;
    case (PLAN_NODE_TYPE_INDEXSCAN):
        ret = new IndexScanPlanNode();
    break;
    case (PLAN_NODE_TYPE_NESTLOOP):
        ret = new NestLoopPlanNode();
    break;
    case (PLAN_NODE_TYPE_NESTLOOPINDEX):
        ret = new NestLoopIndexPlanNode();
    break;
    case (PLAN_NODE_TYPE_UPDATE):
        ret = new UpdatePlanNode();
    break;
    case (PLAN_NODE_TYPE_INSERT):
        ret = new InsertPlanNode();
    break;
    case (PLAN_NODE_TYPE_DELETE):
        ret = new DeletePlanNode();
    break;
    case (PLAN_NODE_TYPE_HASHAGGREGATE):
    case (PLAN_NODE_TYPE_AGGREGATE):
    ret = new AggregatePlanNode(type);
    break;
    case (PLAN_NODE_TYPE_UNION):
        ret = new UnionPlanNode();
    break;
    case (PLAN_NODE_TYPE_ORDERBY):
        ret = new OrderByPlanNode();
    break;
    case (PLAN_NODE_TYPE_PROJECTION):
        ret = new ProjectionPlanNode();
    break;
    case (PLAN_NODE_TYPE_MATERIALIZE):
        ret = new MaterializePlanNode();
    break;
    case (PLAN_NODE_TYPE_SEND):
        ret = new SendPlanNode();
    break;
    case (PLAN_NODE_TYPE_LIMIT):
        ret = new LimitPlanNode();
    break;
    case (PLAN_NODE_TYPE_DISTINCT):
        ret = new DistinctPlanNode();
    break;
    case (PLAN_NODE_TYPE_RECEIVE):
        ret = new ReceivePlanNode();
    break;
     */

    default: {
      throw PlannerException("Invalid PlanNode type : " +
                             PlanNodeTypeToString(type));
    }
  }
  // TODO Uncomment after fixing compilation error.
  // LOG4CXX_TRACE(logger, "created plannode : " << typeid(*ret).name());

  return (ret);
}

}  // namespace planner
}  // namespace peloton
