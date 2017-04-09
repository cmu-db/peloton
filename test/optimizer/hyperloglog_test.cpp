#include "common/harness.h"

#include "common/logger.h"
#include "optimizer/stats/hyperloglog.h"
#include "type/value_factory.h"

#include <string>

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class HyperLogLogTests : public PelotonTest {};

TEST_F(HyperLogLogTests, IntegerValueTests) {

  HyperLogLog hll{};

  for (int i = 0; i < 10; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i);
    hll.AddValue(v);
  }
  EXPECT_LE(hll.EstimateCardinality(), 10);
  EXPECT_GE(hll.EstimateCardinality(), 9);
}

TEST_F(HyperLogLogTests, DecimalValueTests) {
  HyperLogLog hll{};

  for (int i = 0; i < 10; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(i * 1.5);
    hll.AddValue(v);
  }
  EXPECT_LE(hll.EstimateCardinality(), 10);
  EXPECT_GE(hll.EstimateCardinality(), 9);
}

TEST_F(HyperLogLogTests, VarcharValueTests) {
  HyperLogLog hll{};

  for (int i = 42; i < 52; i++) {
    type::Value v = type::ValueFactory::GetVarcharValue(std::to_string(i));
    hll.AddValue(v);
  }
  EXPECT_LE(hll.EstimateCardinality(), 10);
  EXPECT_GE(hll.EstimateCardinality(), 9);
}

// TODO: Test NULL
TEST_F(HyperLogLogTests, NullValueTests) {
}

} /* namespace test */
} /* namespace peloton */
