//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// plan_test.cpp
//
// Identification: test/optimizer/plan_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#include "optimizer_test_util.cpp"
#include "planner/abstract_scan_plan.h"

namespace peloton {
namespace test {

class PlanTest : public OptimizerTestUtil {};


// Tests cost model outputs identical plans regardless of table orderings
TEST_F(PlanTest, PlanEqualityTest) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::POSTGRES);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  int test1_table_size = 10;
  int test2_table_size = 100;
  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);

  // Generate queries with names in different order
  auto query1 = "SELECT test1.a, test2.a FROM test1,test2 WHERE test1.a = test2.a";
  auto query2 = "SELECT test1.a, test2.a FROM test2,test1 WHERE test1.a = test2.a";

  // Generate two plans
  auto plan1 = OptimizerTestUtil::GeneratePlan(query1);
  auto plan2 = OptimizerTestUtil::GeneratePlan(query2);

  EXPECT_EQ(*plan1, *plan2);
}


TEST_F(PlanTest, PostgresTwoJoinOrderTestSmall) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::POSTGRES);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  int test1_table_size = 10;
  int test2_table_size = 100;
  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);


  // Generate query
  auto query = OptimizerTestUtil::CreateTwoWayJoinQuery(test1_table_name, test2_table_name, "a", "a");

  auto plan = OptimizerTestUtil::GeneratePlan(query);

  EXPECT_EQ(PlanNodeType::HASHJOIN, plan->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren().size());


  // Get Left Scan
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  auto left_scan = dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[0].get());

  // Get Right Scan
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());
  auto right_scan =
      dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[1]->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, right_scan->GetPlanNodeType());

  // Check we build hash table on larger table (right table), and probe with smaller table (left table)
  EXPECT_EQ(test1_table_name, left_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test2_table_name, right_scan->GetTable()->GetName().c_str());
}

// With trivial model, ordering of tables in join should be reversed as both orderings have the same cost, however
// test2 x test1 is explored after test1 x test2, so we pick the most previously explored one.
TEST_F(PlanTest, TrivialTwoJoinOrderTestSmall) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::TRIVIAL);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  int test1_table_size = 10;
  int test2_table_size = 100;
  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);


  // Generate query
  auto query = OptimizerTestUtil::CreateTwoWayJoinQuery(test2_table_name, test1_table_name, "a", "a");

  auto plan = OptimizerTestUtil::GeneratePlan(query);

  EXPECT_EQ(PlanNodeType::HASHJOIN, plan->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren().size());


  // Get Left Scan
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  auto left_scan = dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[0].get());

  // Get Right Scan
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());
  auto right_scan =
      dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[1]->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, right_scan->GetPlanNodeType());

  // Check we build hash table on larger table (right table), and probe with smaller table (left table)
  EXPECT_EQ(test1_table_name, left_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test2_table_name, right_scan->GetTable()->GetName().c_str());
}


// Tests that since the left table is a single tuple, we just use nested loop
TEST_F(PlanTest, TrivialTwoJoinOrderTestSmall2) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::TRIVIAL);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  int test1_table_size = 1;
  int test2_table_size = 100;
  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);


  // Generate query
  auto query = OptimizerTestUtil::CreateTwoWayJoinQuery(test1_table_name, test2_table_name, "a", "a");

  auto plan = OptimizerTestUtil::GeneratePlan(query);

  EXPECT_EQ(PlanNodeType::NESTLOOP, plan->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren().size());


  // Get Left Scan
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  auto left_scan = dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[0].get());

  // Get Right Scan
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[1]->GetPlanNodeType());
  EXPECT_EQ(0, plan->GetChildren()[1]->GetChildren().size());
  auto right_scan =
      dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[1].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, right_scan->GetPlanNodeType());

  // Check we build hash table on larger table (right table), and probe with smaller table (left table)
  EXPECT_EQ(test1_table_name, left_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test2_table_name, right_scan->GetTable()->GetName().c_str());
}

TEST_F(PlanTest, PostgresTwoJoinOrderTestLarge) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::POSTGRES);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  int test1_table_size = 10000;
  int test2_table_size = 1000;
  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);


  // Generate query
  auto query = OptimizerTestUtil::CreateTwoWayJoinQuery(test1_table_name, test2_table_name, "a", "a");

  auto plan = OptimizerTestUtil::GeneratePlan(query);

  EXPECT_EQ(PlanNodeType::HASHJOIN, plan->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren().size());


  // Get Left Scan
  EXPECT_EQ(PlanNodeType::SEQSCAN, plan->GetChildren()[0]->GetPlanNodeType());
  EXPECT_EQ(0, plan->GetChildren()[0]->GetChildren().size());
  auto left_scan = dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[0].get());

  // Get Right Scan
  EXPECT_EQ(PlanNodeType::HASH, plan->GetChildren()[1]->GetPlanNodeType());
  EXPECT_EQ(1, plan->GetChildren()[1]->GetChildren().size());
  auto right_scan =
      dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[1]->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, right_scan->GetPlanNodeType());

  // Check we build hash table on larger table (right table), and probe with smaller table (left table)
  EXPECT_EQ(test2_table_name, left_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test1_table_name, right_scan->GetTable()->GetName().c_str());
}

TEST_F(PlanTest, PostgresThreeJoinOrderTestSmall) {

  // Set cost model to postgres cost model
  OptimizerTestUtil::SetCostModel(optimizer::CostModels::POSTGRES);

  // Populate Tables and run Analyze
  std::string test1_table_name = "test1";
  std::string test2_table_name = "test2";
  std::string test3_table_name = "test3";
  int test1_table_size = 10;
  int test2_table_size = 100;
  int test3_table_size = 1000;

  OptimizerTestUtil::CreateTable(test1_table_name, test1_table_size);
  OptimizerTestUtil::CreateTable(test2_table_name, test2_table_size);
  OptimizerTestUtil::CreateTable(test3_table_name, test3_table_size);

  OptimizerTestUtil::AnalyzeTable(test1_table_name);
  OptimizerTestUtil::AnalyzeTable(test2_table_name);
  OptimizerTestUtil::AnalyzeTable(test3_table_name);


  // Generate query
  auto query = OptimizerTestUtil::CreateThreeWayJoinQuery(test2_table_name,
                                                          test3_table_name,
                                                          test1_table_name,
                                                          "a", "a", "a");

  auto plan = OptimizerTestUtil::GeneratePlan(query);

  EXPECT_EQ(PlanNodeType::NESTLOOP, plan->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren().size());


  OptimizerTestUtil::PrintPlan(plan);


  // Get Left Scan
  auto left_scan = dynamic_cast<peloton::planner::AbstractScan *>(plan->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, left_scan->GetPlanNodeType());

  // Get right join Scan
  EXPECT_EQ(PlanNodeType::HASHJOIN, plan->GetChildren()[1]->GetPlanNodeType());
  EXPECT_EQ(2, plan->GetChildren()[1]->GetChildren().size());
  auto right_join = plan->GetChildren()[1].get();

  // Get Middle Scan
  auto middle_scan = dynamic_cast<peloton::planner::AbstractScan *>(right_join->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, middle_scan->GetPlanNodeType());

  // Get Right Scan
  EXPECT_EQ(PlanNodeType::HASH, right_join->GetChildren()[1]->GetPlanNodeType());
  auto right_scan = dynamic_cast<peloton::planner::AbstractScan *>(right_join->GetChildren()[1]->GetChildren()[0].get());
  EXPECT_EQ(PlanNodeType::SEQSCAN, right_scan->GetPlanNodeType());

  // Optimal Should be: (test2) x (test1 x test3)
  EXPECT_EQ(test2_table_name, left_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test1_table_name, middle_scan->GetTable()->GetName().c_str());
  EXPECT_EQ(test3_table_name, right_scan->GetTable()->GetName().c_str());
}


}
}