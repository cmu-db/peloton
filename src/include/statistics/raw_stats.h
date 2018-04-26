//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// raw_stats.h
//
// Identification: src/statistics/backend_stats_context.h

// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "statistics/stats_collector"
#include "statistics/point_metric.h"
#include "statistics/interval_metric.h"

namespace peloton {

namespace stats {

class RawStats {
  public:
    RawStats();
    ~RawStats();
    void RegisterMetrics(StatsCollector& collector);

  private:
    // point metrics
    std::shared_ptr<PointMetric> txn_commits_;

    std::shared_ptr<PointMetric> txn_aborts_;

    std::shared_ptr<PointMetric> table_reads_;

    std::shared_ptr<PointMetric> table_updates_;

    std::shared_ptr<PointMetric> table_inserts_;

    std::shared_ptr<PointMetric> table_deletes_;

    std::shared_ptr<PointMetric> table_memory_alloc_;

    std::shared_ptr<PointMetric> table_memory_usage_;

    std::shared_ptr<PointMetric> index_reads_;

    std::shared_ptr<PointMetric> index_updates_;

    std::shared_ptr<PointMetric> index_inserts_;

    std::shared_ptr<PointMetric> index_deletes_;

    // interval metrics
    std::shared_ptr<IntervalMetric> query_latencies_;

    std::shared_ptr<IntervalMetric> txn_latencies_;

    std::shared_ptr<IntervalMetric> query_processor_times_;
};

}  // namespace stats
}  // namespace peloton
