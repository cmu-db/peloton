//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// column_stats_collector_test.cpp
//
// Identification: test/optimizer/column_stats_collector_test.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include <vector>

#include "common/logger.h"
#include "optimizer/stats/column_stats_collector.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

#define private public
#define TEST_OID 0

namespace peloton {
namespace test {

using namespace optimizer;

class ColumnStatsCollectorTests : public PelotonTest {};

// Basic test with tiny dataset.
TEST_F(ColumnStatsCollectorTests, BasicTest) {
  ColumnStatsCollector colstats{TEST_OID, TEST_OID, TEST_OID,
                                type::TypeId::INTEGER, ""};
  // Edge case
  EXPECT_EQ(colstats.GetFracNull(), 0);  // Should also log error
  EXPECT_EQ(colstats.GetCardinality(), 0);
  EXPECT_EQ(colstats.GetHistogramBound().size(), 0);
  EXPECT_EQ(colstats.GetCommonValueAndFrequency().size(), 0);

  for (int i = 0; i < 10; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i);
    colstats.AddValue(v);
  }
  EXPECT_EQ(colstats.GetCardinality(), 10);
  EXPECT_EQ(colstats.GetFracNull(), 0);
  EXPECT_GE(colstats.GetHistogramBound().size(), 0);
}

// Test categorical values. Categorical data refers to data that
// are not compariable but still hashable.
TEST_F(ColumnStatsCollectorTests, DistinctValueTest) {
  ColumnStatsCollector colstats{TEST_OID, TEST_OID, TEST_OID,
                                type::TypeId::BOOLEAN, ""};
  for (int i = 0; i < 1250; i++) {
    type::Value v = type::ValueFactory::GetBooleanValue(i % 5 == 0);
    colstats.AddValue(v);
  }
  EXPECT_EQ(colstats.GetCardinality(), 2);
  EXPECT_EQ(colstats.GetFracNull(), 0);
  EXPECT_EQ(colstats.GetHistogramBound().size(), 0);  // No histogram dist
}

// Test dataset with extreme distribution.
// More specifically distribution with large amount of data at tail
// with single continuous value to the left of tail.
// This test is commented out because it's a performance test and takes long time.
// TEST_F(ColumnStatsCollectorTests, SkewedDistTest) {
//   ColumnStatsCollector colstats{TEST_OID, TEST_OID, TEST_OID,
//                                 type::TypeId::BIGINT, ""};
//   int big_int = 1234567;
//   int height = 100000;
//   int n = 10;
//   // Build up extreme tail distribution.
//   for (int i = 1; i <= n; i++) {
//     type::Value v = type::ValueFactory::GetBigIntValue(i * big_int);
//     for (int j = 0; j < height; j++) {
//       colstats.AddValue(v);
//     }
//   }
//   EXPECT_EQ(colstats.GetFracNull(), 0);
//   EXPECT_EQ(colstats.GetCardinality(), 10);
//   EXPECT_GE(colstats.GetHistogramBound().size(), 0);
//   EXPECT_LE(colstats.GetHistogramBound().size(), 255);
//   // Add head distribution
//   for (int i = 0; i < big_int; i++) {
//     type::Value v = type::ValueFactory::GetBigIntValue(i);
//     colstats.AddValue(v);
//   }
//   EXPECT_EQ(colstats.GetFracNull(), 0);
//   uint64_t cardinality = colstats.GetCardinality();
//   double error = colstats.GetCardinalityError();
//   int buffer = 30000;  // extreme case error buffer
//   EXPECT_GE(cardinality, (big_int + 10) * (1 - error) - buffer);
//   EXPECT_LE(cardinality, (big_int + 10) * (1 + error) + buffer);
//   EXPECT_GE(colstats.GetHistogramBound().size(), 0);
//   // test null
//   type::Value null = type::ValueFactory::GetNullValueByType(type::TypeId::BIGINT);
//   colstats.AddValue(null);
//   EXPECT_GE(colstats.GetFracNull(), 0);
// }

// Test double values.
TEST_F(ColumnStatsCollectorTests, DecimalTest) {
  ColumnStatsCollector colstats{0, 0, 0, type::TypeId::DECIMAL, ""};
  for (int i = 0; i < 1000; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(4.1525);
    colstats.AddValue(v);
  }
  type::Value v1 = type::ValueFactory::GetDecimalValue(7.12);
  type::Value v2 = type::ValueFactory::GetDecimalValue(10.25);
  colstats.AddValue(v1);
  colstats.AddValue(v2);
  EXPECT_EQ(colstats.GetCardinality(), 3);
}

}  // namespace test
}  // namespace peloton
