//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counter_metric.h
//
// Identification: src/backend/statistics/counter_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <mutex>
#include <map>
#include <vector>
#include <iostream>
#include <string>
#include <sstream>
#include <boost/circular_buffer.hpp>

#include "backend/common/timer.h"
#include "backend/common/types.h"
#include "backend/statistics/abstract_metric.h"

//===--------------------------------------------------------------------===//
// GUC Variables
//===--------------------------------------------------------------------===//


namespace peloton {
namespace stats {


struct LatencyMeasurements {
  double average_   = 0.0;
  double min_       = 0.0;
  double max_       = 0.0;
  double median_    = 0.0;
  double perc_25th_ = 0.0;
  double perc_75th_ = 0.0;
  double perc_99th_ = 0.0;
};

/**
 * Metric as a counter. E.g. # txns committed
 */
class LatencyMetric : public AbstractMetric {
 public:

  LatencyMetric(MetricType type, size_t max_history);

  inline void Reset() {
    latencies_.clear();
    timer_ms_.Reset();
  }

  inline void StartTimer() {
    timer_ms_.Reset();
    timer_ms_.Start();
  }

  inline void RecordLatency() {
    timer_ms_.Stop();
    double latency_value = timer_ms_.GetDuration();
    // Record this latency value only if we can do so without blocking.
    {
      std::unique_lock<std::mutex> lock(latency_mutex_, std::defer_lock);
      if (lock.try_lock()) {
        latencies_.push_back(latency_value);
      }
    }
  }

  LatencyMeasurements ComputeLatencies();

  void Aggregate(AbstractMetric &source);

  inline std::string ToString() {
    std::stringstream ss;
    LatencyMeasurements latencies = ComputeLatencies();
    ss << "TXN LATENCY (ms): [ ";
    ss << "average=" << latencies.average_;
    ss << ", min=" << latencies.min_;
    ss << ", 25th-%-tile=" << latencies.perc_25th_;
    ss << ", median=" << latencies.median_;
    ss << ", 75th-%-tile=" << latencies.perc_75th_;
    ss << ", 99th-%-tile=" << latencies.perc_99th_;
    ss << ", max=" << latencies.max_;
    ss << " ]" << std::endl;
    return ss.str();
  }

  boost::circular_buffer<double> Copy();

 private:
  boost::circular_buffer<double> latencies_;
  Timer<std::ratio<1, 1000>> timer_ms_;
  size_t max_history_;
  std::mutex latency_mutex_;

};

}  // namespace stats
}  // namespace peloton
