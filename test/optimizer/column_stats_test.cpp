#include "common/harness.h"

#include <vector>

#include "common/logger.h"
#include "optimizer/stats/column_stats.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

#define private public
#define TEST_OID 0

namespace peloton {
namespace test {

using namespace optimizer;

class ColumnStatsTests : public PelotonTest {};

using ValueFrequencyPair = std::pair<type::Value, double>;

void PrintCommonValueAndFrequency(
    std::vector<std::pair<type::Value, double>> valfreq) {
  for (auto const& p : valfreq) {
    LOG_INFO("\n [Print k Values] %s, %f", p.first.GetInfo().c_str(), p.second);
  }
}

TEST_F(ColumnStatsTests, BasicTest) {
  ColumnStats colstats{TEST_OID, TEST_OID, TEST_OID,
                       type::Type::TypeId::INTEGER};
  for (int i = 0; i < 10; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i);
    colstats.AddValue(v);
  }
  EXPECT_EQ(colstats.GetCardinality(), 10);
  EXPECT_EQ(colstats.GetFracNull(), 0);
  EXPECT_EQ(colstats.GetHistogramBound().size() + 1, colstats.num_bins);
  EXPECT_EQ(colstats.GetCommonValueAndFrequency().size(), colstats.top_k);
}

// Test categorical values
TEST_F(ColumnStatsTests, DistinctValueTest) {
  ColumnStats colstats{TEST_OID, TEST_OID, TEST_OID,
                       type::Type::TypeId::BOOLEAN};
  for (int i = 0; i < 1250; i++) {
    type::Value v = type::ValueFactory::GetBooleanValue(i % 5 == 0);
    colstats.AddValue(v);
  }
  EXPECT_EQ(colstats.GetCardinality(), 2);
  EXPECT_EQ(colstats.GetFracNull(), 0);
  EXPECT_EQ(colstats.GetHistogramBound().size(),
            0);  // No histogram for categorical data
  std::vector<ValueFrequencyPair> valfreq =
      colstats.GetCommonValueAndFrequency();
  // EXPECT_EQ(valfreq.size(), 2);
  // PrintCommonValueAndFrequency(colstats.GetCommonValueAndFrequency());
}

// All stats collectors should work with trivial case
TEST_F(ColumnStatsTests, TrivialValueTest) {
  ColumnStats colstats{TEST_OID, TEST_OID, TEST_OID, type::Type::TypeId::ARRAY};
  EXPECT_EQ(colstats.GetFracNull(), 0);  // Should also log error
  EXPECT_EQ(colstats.GetCardinality(), 0);
  EXPECT_EQ(colstats.GetHistogramBound().size(), 0);
  EXPECT_EQ(colstats.GetCommonValueAndFrequency().size(), 0);
}

TEST_F(ColumnStatsTests, LeftSkewedDistTest) {
  ColumnStats colstats{TEST_OID, TEST_OID, TEST_OID,
                       type::Type::TypeId::BIGINT};
  int big_int = 12345;
  for (int i = 1; i <= 10; i++) {
    type::Value v = type::ValueFactory::GetBigIntValue(i * big_int);
    for (int j = 0; j < 100000; j++) {
      colstats.AddValue(v);
    }
  }
  EXPECT_EQ(colstats.GetFracNull(), 0);
  EXPECT_EQ(colstats.GetCardinality(), 10);
  EXPECT_EQ(colstats.GetHistogramBound().size() + 1, 10);
  EXPECT_EQ(colstats.GetCommonValueAndFrequency().size(), 10);
  for (int i = big_int + 1; i < 2 * big_int; i++) {
    type::Value v = type::ValueFactory::GetBigIntValue(i);
    colstats.AddValue(v);
  }
  EXPECT_EQ(colstats.GetFracNull(), 0);
  // HLL tends to underestimate
  EXPECT_GE(colstats.GetCardinality(), big_int + 100);
  EXPECT_LE(colstats.GetCardinality(), 3 * big_int - 100);
  EXPECT_EQ(colstats.GetHistogramBound().size() + 1, 10);
  EXPECT_EQ(colstats.GetCommonValueAndFrequency().size(), 10);
}

TEST_F(ColumnStatsTests, DecimalTest) {
  ColumnStats colstats{0, 0, 0, type::Type::TypeId::DECIMAL};
  for (int i = 0; i < 1000; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
    colstats.AddValue(v);
  }
  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
  colstats.AddValue(v1);
  colstats.AddValue(v2);
  EXPECT_EQ(colstats.GetCardinality(), 3);
  for (int i = 0; i < 100; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(5.1525 + i);
    colstats.AddValue(v);
    colstats.AddValue(v);
    colstats.AddValue(v);
  }
  std::vector<std::pair<type::Value, double>> valfreq =
      colstats.GetCommonValueAndFrequency();
  // EXPECT_EQ(valfreq.size(), 10);
}
} /* namespace test */
} /* namespace peloton */
