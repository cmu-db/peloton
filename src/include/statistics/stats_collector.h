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

#include "common/internal_types.h"
#include "statistics/point_metric.h"
#include "statistics/internal_metric.h"

namespace peloton {

namespace stats {

// actual singleton
class StatsCollector {
  public:
    static StatsCollector* GetInstance();

    ~StatsCollector();

    void OnRead(oid_t id);

    void OnUpdate(oid_t id);

    void OnInsert(oid_t id);

    void OnDelete(oid_t id);

    void OnCommit(oid_t id);

    void OnAbort(oid_t id);

    void OnQueryStart();

    void OnQueryEnd();

    void OnTxnStart();

    void OnTxnEnd();

    void RegisterPointMetric(std::shared_ptr<PointMetric> metric,
                             CollectionPointType point_type);

    void RegisterIntervalMetric(std::shared_ptr<IntervalMetric> metric,
                                CollectionPointType start_point_type,
                                CollectionPointType end_point_type);

  private:
    StatsCollector();

    // iterate over a point's metrics queue
    CollectAtPoint(std::vector<std::shared_ptr<PointMetric>> queue, oid_t id);

    CollectAtStart(std::vector<std::shared_ptr<IntervalMetric>> queue);

    CollectAtEnd(std::vector<std::shared_ptr<IntervalMetric>> queue);

    // we have one queue per collection point
    std::vector<std::shared_ptr<PointMetric>> on_read_queue_;

    std::vector<std::shared_ptr<PointMetric>> on_update_queue_;

    std::vector<std::shared_ptr<PointMetric>> on_insert_queue_;

    std::vector<std::shared_ptr<PointMetric>> on_delete_queue_;

    std::vector<std::shared_ptr<PointMetric>> on_commit_queue_;

    std::vector<std::shared_ptr<PointMetric>> on_abort_queue_;

    std::vector<std::shared_ptr<IntervalMetric>> on_query_start_;

    std::vector<std::shared_ptr<IntervalMetric>> on_query_end_;

    std::vector<std::shared_ptr<IntervalMetric>> on_txn_start_queue_;

    std::vector<std::shared_ptr<IntervalMetric>> on_txn_end_queue_;
};

}  // namespace stats
}  // namespace peloton
