#include "common/harness.h"

#define private public

#include "optimizer/histogram.h"

namespace peloton {
namespace test {

using namespace optimizer;

class HistogramTests : public PelotonTest {};

TEST_F(HistogramTests, SimpleHistogramTest) {

  Histogram h{5};
	for (int i = 1; i <= 10; i++) {
		h.Update(i);
	}
	h.Print();

	std::cout << h.Sum(3) << std::endl;
	std::cout << h.Sum(3.5) << std::endl;
	std::cout << h.Sum(4) << std::endl;

  h.Uniform(4);
}

} /* namespace test */
} /* namespace peloton */
