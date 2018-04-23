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

namespace peloton {

namespace stats {

// actual singleton
class StatsCollector {
  public:
    static StatsCollector* GetInstance();

    ~StatsCollector();

    // TODO: add appropriate args for each collection point
    void OnRead();

    void OnUpdate();

    void OnInsert();

    void OnDelete();

    void OnTxnStart();

    void OnCommit();

    void OnAbort();

    void OnQueryStart();

    void OnQueryEnd();

    void RegisterPointMetric(/* specify collection point */);

    void RegisterIntervalMetric(/* specify begin and end points */);

  private:
    StatsCollector();

    // iterate over a point's metrics queue
    CollectAtPoint(std::vector<AbstractMetric> queue);

    // we have one queue per collection point
    std::vector<PointMetric> on_read_queue_;

    std::vector<PointMetric> on_update_queue_;

    std::vector<PointMetric> on_insert_queue_;

    std::vector<PointMetric> on_delete_queue_;

    std::vector<IntervalMetric> on_txn_start_queue_;

    // could collect both point and interval metrics
    std::vector<AbstractMetric> on_commit_queue_;

    std::vector<AbstractMetric> on_abort_queue_;

    std::vector<IntervalMetric> on_query_start_;

    std::vector<IntervalMetric> on_query_end_;
};

}  // namespace stats
}  // namespace peloton
