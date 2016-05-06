//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric.h
//
// Identification: src/backend/statistics/abstract_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>

#include "backend/common/types.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


/**
 * Abstract class for metrics 
 */
class AbstractMetric {
 public:

  AbstractMetric(MetricType type_);
  virtual ~AbstractMetric();

  const inline MetricType& GetType() const {
    return type;
  }

  virtual void Reset() = 0;

  virtual void Aggregate(AbstractMetric &source) = 0;

 private:
  MetricType type;

};

}  // namespace stats
}  // namespace peloton
