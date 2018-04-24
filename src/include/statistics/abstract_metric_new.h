//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric_new.h
//
// Identification: src/statistics/abstract_metric_new.h
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
 */
class AbstractMetricNew : public Printable {
 public:
  AbstractMetricNew(MetricType type_);
  virtual ~AbstractMetricNew();

  virtual void Reset() = 0;

  virtual const std::string GetInfo() const = 0;
};

}  // namespace stats
}  // namespace peloton
