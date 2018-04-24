//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// interval_metric.h
//
// Identification: src/statistics/interval_metric.h
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.h"
#include "common/lock_free_array.h"
#include "statistics/abstract_metric_new.h"

namespace peloton {
namespace stats {

/**
Each instance of this class is a mapping from either
database/table/index oids to counters,
eg database commits or table insersts
 */
class IntervalMetric : public AbstractMetricNew {
  public:
    PointMetric();

    void Reset();

    const std::string GetInfo() const;

    // Do any work necessary at interval start,
    // eg start a timer
    void Init();

    // Collect at interval end
    void Collect();

  private:
    // TODO: put storage into lower abstraction class
    // append-only list of durations measured
    LockFreeArray<double> durations_;
};

}  // namespace stats
}  // namespace peloton
