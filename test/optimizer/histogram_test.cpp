//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// histogram_test.cpp
//
// Identification: test/optimizer/histogram_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <random>

#include "optimizer/stats/histogram.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

using namespace optimizer;

class HistogramTests : public PelotonTest {};

// 100k values with uniform distribution from 1 to 100.
TEST_F(HistogramTests, UniformDistTest) {
  Histogram h{};
  int n = 100000;
  std::default_random_engine generator;
  std::uniform_int_distribution<int> distribution(1, 100);
  for (int i = 0; i < n; i++) {
    int number = distribution(generator);
    h.Update(number);
  }
  std::vector<double> res = h.Uniform();
  for (int i = 1; i < 100; i++) {
    // Should have each value as histogram bound.
    EXPECT_EQ(i, std::floor(res[i - 1]));
  }
}

// Gaussian distribution with 100k values.
TEST_F(HistogramTests, GaussianDistTest) {
  Histogram h{};
  int n = 100000;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::normal_distribution<> distribution(0, 10);
  for (int i = 0; i < n; i++) {
    int number = distribution(generator);
    h.Update(number);
  }
  std::vector<double> res = h.Uniform();
  // This should have around 68% data in one stdev [-10, 10]
  int count = 0;
  for (double x : res)
    if (x >= -10 && x <= 10) count++;
  EXPECT_GE(count, res.size() * 0.68);
}

// Log-normal distribution with 100k values.
TEST_F(HistogramTests, LeftSkewedDistTest) {
  Histogram h{};
  int n = 100000;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::lognormal_distribution<> distribution(0, 1);
  for (int i = 0; i < n; i++) {
    int number = distribution(generator);
    h.Update(number);
  }
  std::vector<double> res = h.Uniform();
}

// Exponential distribution.
TEST_F(HistogramTests, ExponentialDistTest) {
  Histogram h{};
  int n = 100000;
  double lambda = 1;
  std::random_device rd;
  std::mt19937 generator(rd());
  std::exponential_distribution<> distribution(lambda);
  for (int i = 0; i < n; i++) {
    int number = distribution(generator);
    h.Update(number);
  }
  std::vector<double> res = h.Uniform();
  // ln 2 / lambda is the mean
  double threshold = std::log(2) / lambda;
  int count = 0;
  for (double x : res)
    if (x < threshold) count++;
  EXPECT_GE(count, res.size() * 0.5);
}

// Handle error cases correctly.
TEST_F(HistogramTests, ValueTypeTest) {
  Histogram h{};
  // Does not support varchar
  type::Value s = type::ValueFactory::GetVarcharValue("test");
  h.Update(s);
  EXPECT_EQ(h.GetTotalValueCount(), 0);
  // Handle timestamp value correctly
  type::Value timestamp = type::ValueFactory::GetTimestampValue(1493094993);
  h.Update(timestamp);
  EXPECT_EQ(h.GetTotalValueCount(), 1);
  // Handle integer value correctly
  type::Value big_int = type::ValueFactory::GetBigIntValue(12345654321);
  h.Update(big_int);
  EXPECT_EQ(h.GetTotalValueCount(), 2);
  // Does not support bool
  type::Value b = type::ValueFactory::GetBooleanValue(true);
  h.Update(b);
  EXPECT_EQ(h.GetTotalValueCount(), 2);
  // Handle decimal value correctly
  type::Value decimal = type::ValueFactory::GetDecimalValue(123.12);
  h.Update(decimal);
  EXPECT_EQ(h.GetTotalValueCount(), 3);
  // Handle null value correctly
  type::Value invalid =
      type::ValueFactory::GetNullValueByType(type::TypeId::INTEGER);
  h.Update(invalid);
  EXPECT_EQ(h.GetTotalValueCount(), 3);
  // uniform should handle small dataset
  std::vector<double> res = h.Uniform();
  EXPECT_GE(res.size(), 0);
}

TEST_F(HistogramTests, SumTest) {
  Histogram h{};
  h.Uniform();
  EXPECT_EQ(h.Sum(0), 0);
  h.Update(5);
  EXPECT_EQ(h.Sum(3), 0);
  EXPECT_EQ(h.Sum(4), 0);
  EXPECT_EQ(h.Sum(5), 1);
  EXPECT_EQ(h.Sum(6), 1);
}

}  // namespace test
}  // namespace peloton
