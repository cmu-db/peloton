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
#include "backend/networking/rpc_server.h"
#include "backend/networking/peloton_service.h"
#include "backend/planner/seq_scan_plan.h"

namespace peloton {
namespace test {

class RpcQueryPlanTests : public PelotonTest {};

TEST_F(RpcQueryPlanTests, BasicTest) {

    peloton::planner::SeqScanPlan mapped_plan_ptr;

    const peloton::PlanNodeType type = mapped_plan_ptr.GetPlanNodeType();
    peloton::networking::QueryPlanExecRequest request;
    request.set_plan_type(static_cast<int>(type));

    peloton::CopySerializeOutput output_plan;
    bool serialize = mapped_plan_ptr.SerializeTo(output_plan);
    // Becuase the plan is not completed, so it is false
    EXPECT_EQ(serialize, false);
}

}
}


