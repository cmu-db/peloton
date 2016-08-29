//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// rpc_queryplan_test.cpp
//
// Identification: test/networking/rpc_queryplan_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "common/harness.h"
#include "networking/rpc_server.h"
#include "networking/peloton_service.h"
#include "planner/seq_scan_plan.h"

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
