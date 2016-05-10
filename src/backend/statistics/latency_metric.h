//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.h
//
// Identification: src/backend/statistics/latency_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <boost/circular_buffer.hpp>
#include <string>
#include <sstream>

#include "backend/common/timer.h"
#include "backend/common/types.h"
#include "backend/statistics/abstract_metric.h"

namespace peloton {
namespace stats {

// Container for different latency measurements
struct LatencyMeasurements {
  double average_ = 0.0;
  double min_ = 0.0;
  double max_ = 0.0;
  double median_ = 0.0;
  double perc_25th_ = 0.0;
  double perc_75th_ = 0.0;
  double perc_99th_ = 0.0;
};

/**
 * Metric for storing raw latency values and computing
 * latency measurements.
 */
class LatencyMetric : public AbstractMetric {
 public:
  LatencyMetric(MetricType type, size_t max_history);

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline void Reset() {
    latencies_.clear();
    timer_ms_.Reset();
  }

  // Starts the timer for the next latency measurement
  inline void StartTimer() {
    timer_ms_.Reset();
    timer_ms_.Start();
  }

  // Stops the latency timer and records the total time elapsed
  inline void RecordLatency() {
    timer_ms_.Stop();
    double latency_value = timer_ms_.GetDuration();
    // Record this latency only if we can do so without blocking.
    // Occasionally losing single latency measurements is fine.
    {
      std::unique_lock<std::mutex> lock(latency_mutex_, std::defer_lock);
      if (lock.try_lock()) {
        latencies_.push_back(latency_value);
      }
    }
  }

  // Computes the latency measurements using the latencies
  // collected so far.
  void ComputeLatencies();

  // Combines the source latency metric with this latency metric
  void Aggregate(AbstractMetric &source);

  // Returns a string representation of this latency metric
  std::string ToString() const;

  // Returns a copy of the latencies collected
  boost::circular_buffer<double> Copy();

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Circular buffer with capacity N that stores the <= N
  // most recent latencies collected
  boost::circular_buffer<double> latencies_;

  // Timer for timing individual latencies
  Timer<std::ratio<1, 1000>> timer_ms_;

  // Stores result of last call to ComputeLatencies()
  LatencyMeasurements latency_measurements_;

  // The maximum number of latencies that can be stored
  // (the capacity size N of the circular buffer)
  size_t max_history_;

  // Protects accesses to the circular buffer
  std::mutex latency_mutex_;
};

}  // namespace stats
}  // namespace peloton
