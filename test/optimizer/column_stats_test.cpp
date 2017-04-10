#include "common/harness.h"

#include <vector>

#include "common/logger.h"
#include "optimizer/stats/column_stats.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

#define private public

namespace peloton {
namespace test {

using namespace optimizer;

class ColumnStatsTests : public PelotonTest {};

TEST_F(ColumnStatsTests, BasicTests) {
  ColumnStats colstats{0, 0, 0, type::Type::TypeId::INTEGER};
  for (int i = 0; i < 100000; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i);
    colstats.AddValue(v);
  }
  // Minimum accuracy requirement
  uint64_t cardinality = colstats.GetCardinality();
  EXPECT_GE(cardinality, 50000);
  EXPECT_LE(cardinality, 150000);
  // Histogram bound
  std::vector<double> bounds = colstats.GetHistogramBound();
  // Null value fraction
  EXPECT_EQ(colstats.GetFracNull(), 0);
  // Common value with frequency
  std::vector<std::pair<type::Value, double>> valfreq =
			colstats.GetCommonValueAndFrequency();
}

TEST_F(ColumnStatsTests, SkewedTests) {
  ColumnStats colstats{0, 0, 0, type::Type::TypeId::DECIMAL};
  for (int i = 0; i < 1000; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
    colstats.AddValue(v);
  }
  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
  colstats.AddValue(v1);
  colstats.AddValue(v2);
  uint64_t cardinality = colstats.GetCardinality();
  EXPECT_GE(cardinality, 1);
  EXPECT_LE(cardinality, 5);
  // Common value with frequency
  std::vector<std::pair<type::Value, double>> valfreq =
			colstats.GetCommonValueAndFrequency();
}

} /* namespace test */
} /* namespace peloton */
