#include "common/harness.h"

#include "common/logger.h"
#include "optimizer/hyperloglog.h"

#include <string>

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class HyperLogLogTests : public PelotonTest {};

TEST_F(HyperLogLogTests, BasicTests) {

  HyperLogLog hll{};

  for (int i = 0 ; i < 10; i++) {
    hll.Add(std::to_string(i).c_str());
  }

  EXPECT_LE(hll.EstimateCardinality(), 10);
  EXPECT_GE(hll.EstimateCardinality(), 9);

}


} /* namespace test */
} /* namespace peloton */
