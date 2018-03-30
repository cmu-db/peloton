//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyperloglog_test.cpp
//
// Identification: test/optimizer/hyperloglog_test.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "common/harness.h"

#include "optimizer/stats/hyperloglog.h"

#include "common/logger.h"
#include "optimizer/stats/column_stats_collector.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// HyperLogLog Tests
//===----------------------------------------------------------------------===//

using namespace optimizer;

class HyperLogLogTests : public PelotonTest {};

// 100k values with 10k distinct.
TEST_F(HyperLogLogTests, SmallDatasetTest1) {
  HyperLogLog hll{};
  int threshold = 100000;
  int ratio = 10;
  double error = hll.RelativeError();
  for (int i = 1; i <= threshold; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i / ratio);
    hll.Update(v);
  }
  uint64_t cardinality = hll.EstimateCardinality();
  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
  LOG_TRACE("Estimated cardinality: %lu", cardinality);
  UNUSED_ATTRIBUTE double true_error =
      (threshold * 1.0 - (int)cardinality) / cardinality;
  LOG_TRACE("Estimated cardinality is [%f] times of real value", true_error);
}

// 100k values with 1k distinct.
// This case HLL does not perform very well.
TEST_F(HyperLogLogTests, SmallDatasetTest2) {
  HyperLogLog hll{};
  int threshold = 100000;
  int ratio = 100;
  double error = hll.RelativeError() + 0.05;
  for (int i = 1; i <= threshold; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i / ratio);
    hll.Update(v);
  }
  uint64_t cardinality = hll.EstimateCardinality();
  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
}

// 100k values with 100 distinct.
TEST_F(HyperLogLogTests, SmallDatasetTest3) {
  HyperLogLog hll{};
  int threshold = 100000;
  int ratio = 1000;
  double error = hll.RelativeError();
  for (int i = 1; i <= threshold; i++) {
    type::Value v =
        type::ValueFactory::GetVarcharValue(std::to_string(i / ratio));
    hll.Update(v);
  }
  uint64_t cardinality = hll.EstimateCardinality();
  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
}

// 100k values with 100k distinct.
TEST_F(HyperLogLogTests, SmallDatasetTest4) {
  HyperLogLog hll{};
  int threshold = 100000;
  int ratio = 1;
  double error = hll.RelativeError();
  for (int i = 1; i <= threshold; i++) {
    type::Value v = type::ValueFactory::GetDecimalValue(i / ratio);
    hll.Update(v);
  }
  uint64_t cardinality = hll.EstimateCardinality();
  EXPECT_LE(cardinality, threshold / ratio * (1 + error));
  EXPECT_GE(cardinality, threshold / ratio * (1 - error));
}

// HLL performance with different precisions.
// In general, the higher the precision, the smaller the error.
TEST_F(HyperLogLogTests, PrecisionTest) {
  int threshold = 100000;
  int ratio = 10;
  HyperLogLog hll_10{10};
  double error_10 = hll_10.RelativeError() + 0.001;
  HyperLogLog hll_14{14};
  double error_14 = hll_14.RelativeError() + 0.001;
  HyperLogLog hll_4{4};
  double error_4 =
      hll_4.RelativeError() + 0.05;  // precision 4 tend to be worse
  for (int i = 1; i <= threshold; i++) {
    type::Value v = type::ValueFactory::GetIntegerValue(i / ratio);
    hll_4.Update(v);
    hll_10.Update(v);
    hll_14.Update(v);
  }
  EXPECT_LE(hll_4.EstimateCardinality(), threshold / ratio * (1 + error_4));
  EXPECT_GE(hll_4.EstimateCardinality(), threshold / ratio * (1 - error_4));

  EXPECT_LE(hll_10.EstimateCardinality(), threshold / ratio * (1 + error_10));
  EXPECT_GE(hll_10.EstimateCardinality(), threshold / ratio * (1 - error_10));

  EXPECT_LE(hll_14.EstimateCardinality(), threshold / ratio * (1 + error_14));
  EXPECT_GE(hll_14.EstimateCardinality(), threshold / ratio * (1 - error_14));
}

// 100M values with 10M distinct. Comment out due to long running time.
// TEST_F(HyperLogLogTests, LargeDatasetTest) {
//   HyperLogLog hll{};
//   int threshold = 100000000;
//   int ratio = 10;
//   double error = hll.RelativeError();
//   for (int i = 1; i <= 100000000; i++) {
//     type::Value v = type::ValueFactory::GetIntegerValue(i / 10);
//     hll.Update(v);
//   }
//   uint64_t cardinality = hll.EstimateCardinality();
//   EXPECT_LE(cardinality, threshold / ratio * (1 + error));
//   EXPECT_GE(cardinality, threshold / ratio * (1 - error));
// }

// Hyperloglog should be able to handle different value types.
TEST_F(HyperLogLogTests, DataTypeTest) {
  HyperLogLog hll{};
  // integer
  type::Value tiny_int = type::ValueFactory::GetTinyIntValue(1);
  hll.Update(tiny_int);
  type::Value timestamp = type::ValueFactory::GetTimestampValue(1493003492);
  hll.Update(timestamp);
  // double
  type::Value decimal = type::ValueFactory::GetDecimalValue(12.9998435);
  hll.Update(decimal);
  // string
  std::string str{"database"};
  type::Value str_val = type::ValueFactory::GetVarcharValue(str);
  hll.Update(str_val);
  // Null
  type::Value null =
      type::ValueFactory::GetNullValueByType(type::TypeId::BOOLEAN);
  hll.Update(str_val);

  hll.EstimateCardinality();
}

}  // namespace test
}  // namespace peloton
