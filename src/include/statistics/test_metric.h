//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// test_metric.h
//
// Identification: src/include/statistics/test_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "abstract_metric.h"

namespace peloton {
namespace stats {
/**
 * @brief raw data type for testing purpose
 */
class TestMetricRawData : public AbstractRawData {
 public:
  /**
   * @brief integrate the count with the number specified
   * @param num number to be integrate
   */
  inline void Integrate(int num) { count_ += num; }

  /**
   * @brief aggregate the counts
   * @param other
   */
  inline void Aggregate(AbstractRawData &other) {
    auto &other_test = dynamic_cast<TestMetricRawData &>(other);
    count_ += other_test.count_;
  }

  void WriteToCatalog() override {}

  const std::string GetInfo() const override { return "test metric"; }

  int count_;
};

class TestMetric : public AbstractMetric<TestMetricRawData> {
 public:
  inline void OnTest(int num) override { GetRawData()->Integrate(num); }
};
}
}