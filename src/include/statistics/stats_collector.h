//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// stats_collector.h
//
// Identification: src/statistics/backend_stats_context.h
//
// Copyright (c) 2017-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <unordered_map>
#include "common/internal_types.h"
#include "statistics/point_metric.h"
#include "statistics/stat_insertion_point.h"
#include "statistics/abstract_metric_new.h"
#include "statistics/database_metric_new.h"

namespace peloton {

namespace stats {

// actual singleton
class StatsCollector {
  public:
    static StatsCollector* GetInstance() {
      static StatsCollector collector;
      return &collector;
    }

    ~StatsCollector() = default;

    template <StatInsertionPoint type, typename... Args>
    void CollectStat(Args... args) {
        for (auto &metric: metric_dispatch_[type])
          metric->OnStatAvailable<type>(args...);
    };

  private:
    StatsCollector() {
        // TODO(tianyu): Write stats to register here
      RegisterMetric<DatabaseMetricNew>
          ({StatInsertionPoint::TXN_ABORT, StatInsertionPoint::TXN_COMMIT});
    }

    template<typename metric>
    void RegisterMetric(std::vector<StatInsertionPoint> points) {
      auto m = std::make_shared<metric>();
      metrics_.push_back(m);
      for (StatInsertionPoint point : points)
        metric_dispatch_[point].push_back(m);
    }

    using MetricList = std::vector<std::shared_ptr<AbstractMetricNew>>
    MetricList metrics_;
    std::unordered_map<StatInsertionPoint, MetricList> metric_dispatch_;
};

}  // namespace stats
}  // namespace peloton
