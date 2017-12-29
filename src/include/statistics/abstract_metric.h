//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.h
//
// Identification: src/statistics/abstract_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.h"
#include "common/printable.h"

namespace peloton {
namespace stats {

/**
 * Abstract class for metrics
 * A metric should be able to:
 * (1) identify its type;
 * (2) print itself (ToString);
 * (3) reset itself;
 * (4) aggregate itself with another source
 *     of the same type.
 */
class AbstractMetric : public Printable {
 public:
  AbstractMetric(MetricType type_);
  virtual ~AbstractMetric();

  const inline MetricType& GetType() const { return type_; }

  virtual void Reset() = 0;

  virtual const std::string GetInfo() const = 0;

  virtual void Aggregate(AbstractMetric& source) = 0;

 private:
  // The type this metric belongs to
  MetricType type_;
};

}  // namespace stats
}  // namespace peloton
