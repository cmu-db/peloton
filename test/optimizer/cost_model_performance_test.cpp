//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cost_test.cpp
//
// Identification: test/optimizer/cost_model_performance_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
namespace peloton {
namespace test {

class CostModelPerformanceTests : public PelotonTest {};

void CreateAndLoadTable() {

  benchmark::tpch::TPCHDatabase::CreateTables();
}

TEST_F(CostModelPerformanceTests, TPCHCostTest) {
  CreateAndLoadTable();

}
}
}