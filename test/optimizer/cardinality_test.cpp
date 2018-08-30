
#include "optimizer_test_util.cpp"

namespace peloton {
namespace test {

  class CardinalityTest : public OptimizerTestUtil {

   public:

    void SetUp() override {
      OptimizerTestUtil::SetUp();
    }

    void TearDown() override {
      OptimizerTestUtil::TearDown();
    }
  };


TEST_F(CardinalityTest, EstimatedCardinalityTest) {

  const std::string test_table_name = "TestTable";
  const int num_rows = 100;
  OptimizerTestUtil::CreateTable(test_table_name, num_rows);

  auto plan = GeneratePlan("SELECT * from " + test_table_name + ";");

  EXPECT_EQ(num_rows, plan->GetCardinality());
}


}
}
