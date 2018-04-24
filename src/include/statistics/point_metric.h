//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// point_metric.h
//
// Identification: src/statistics/point_metric.h
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.h"
#include "common/abstract_metric_new.h"

namespace peloton {
namespace stats {

/**
Each instance of this class is a mapping from either
database/table/index oids to counters,
eg database commits or table insersts
 */
class PointMetric : public AbstractMetricNew {
  public:
    PointMetric();

    void Reset();

    const std::string GetInfo() const;

    void Collect(oid_t id);

  private:
    // TODO: consider making storage a lower abstraction level
    
    // thread-safe map of db oid to atomic counter
    CuckooMap<oid_t, std::atomic<int64_t>> counts_;
};

}  // namespace stats
}  // namespace peloton
