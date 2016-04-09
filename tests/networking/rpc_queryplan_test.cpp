//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// rpc_queryplan.cpp
//
// Identification: /peloton/tests/networking/rpc_queryplan.cpp
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "harness.h"
#include "backend/networking/rpc_utils.h"
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_service.h"
#include "backend/planner/seq_scan_plan.h"

namespace peloton {
namespace test {

class RpcQueryPlanTests : public PelotonTest {};

TEST_F(RpcQueryPlanTests, BasicTest) {

    TupleDesc tuple_desc = new tupleDesc;
    tuple_desc->attrs = NULL;
    tuple_desc->constr = NULL;
    tuple_desc->natts = 0;
    tuple_desc->tdhasoid = true;
    tuple_desc->tdrefcount = 10;
    tuple_desc->tdtypeid = 123;
    tuple_desc->tdtypmod = 789;

    peloton::planner::SeqScanPlan mapped_plan_ptr;

    const peloton::PlanNodeType type = mapped_plan_ptr.GetPlanNodeType();
    peloton::networking::QueryPlanExecRequest request;
    request.set_plan_type(static_cast<int>(type));

    peloton::networking::TupleDescMsg* tuple_desc_msg = request.mutable_tuple_dec();
    peloton::networking::SetTupleDescMsg(tuple_desc, *tuple_desc_msg);

    int atts_count = tuple_desc->natts;
    int repeate_count = tuple_desc_msg->attrs_size();
    EXPECT_EQ(atts_count, repeate_count);

    peloton::networking::TupleDescMsg tdmsg = request.tuple_dec();
    int size1 = tdmsg.natts();
    int size2 = tdmsg.attrs_size();
    EXPECT_EQ(size1, size2);

    peloton::CopySerializeOutput output_plan;
    bool serialize = mapped_plan_ptr.SerializeTo(output_plan);
    // Becuase the plan is not completed, so it is false
    EXPECT_EQ(serialize, false);

    delete tuple_desc;
}

}
}


