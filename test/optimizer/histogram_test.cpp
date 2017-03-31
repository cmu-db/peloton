#include "common/harness.h"

#define private public

#include "optimizer/histogram.h"

namespace peloton {
namespace test {

using namespace optimizer;

class HistogramTests : public PelotonTest {};

TEST_F(HistogramTests, SimpleHistogramTest) {

  EXPECT_EQ(1, 1);
}



} /* namespace test */
} /* namespace peloton */
