//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// histogram_test.cpp
//
// Identification: test/optimizer/histogram_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#define private public

#include "optimizer/stats/histogram.h"

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
  h.Uniform(4);
}

} /* namespace test */
} /* namespace peloton */
