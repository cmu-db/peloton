//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.h
//
// Identification: src/statistics/latency_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "common/timer.h"
#include "common/macros.h"
#include "common/internal_types.h"
#include "common/exception.h"
#include "common/container/circular_buffer.h"
#include "statistics/abstract_metric.h"
#include <mutex>

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

class LatencyMetricRawData : public AbstractRawData {
 public:
  // TODO (Justin): remove hard-coded constant
  // Probably want agg structure to have more capacity
  LatencyMetricRawData(size_t max_history = 100) {
    latencies_.SetCapacity(max_history);
  }

  inline void RecordLatency(const double val) { latencies_.PushBack(val); }

  void Aggregate(AbstractRawData &other) {
    auto &other_latency_metric = dynamic_cast<LatencyMetricRawData &>(other);
    for (double next_latency : other_latency_metric.latencies_) {
      latencies_.PushBack(next_latency);
    }
  }

  void WriteToCatalog();

 private:
  /**
   * @brief Calculate descriptive statistics on raw latency measurements.
   *
   * Should only be called by aggregator thread, after it has aggregated
   * latencies from all worker threads.
   * Only then does it make sense to calculate stats such as min, max, and
   *percentiles.
   */
  LatencyMeasurements DescriptiveFromRaw();

  // Circular buffer with capacity N that stores the <= N
  // most recent latencies collected
  CircularBuffer<double> latencies_;
};

class LatencyMetric : public AbstractMetric<LatencyMetricRawData> {
 public:
  inline void OnQueryBegin() {
    timer_ms_.Reset();
    timer_ms_.Start();
  }

  inline void OnQueryEnd() {
    timer_ms_.Stop();
    GetRawData()->RecordLatency(timer_ms_.GetDuration());
  }

 private:
  // Timer for timing individual latencies
  Timer<std::ratio<1, 1000>> timer_ms_;
};

/**
 * Metric for storing raw latency values and computing
 * latency measurements.
 */
class LatencyMetricOld : public AbstractMetricOld {
 public:
  LatencyMetricOld(MetricType type, size_t max_history);

  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  inline void Reset() {
    latencies_.Clear();
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
        latencies_.PushBack(latency_value);
      }
    }
  }

  // Returns the first latency value recorded
  inline double GetFirstLatencyValue() {
    PELOTON_ASSERT(latencies_.begin() != latencies_.end());
    return *(latencies_.begin());
  }

  // Computes the latency measurements using the latencies
  // collected so far.
  void ComputeLatencies();

  // Combines the source latency metric with this latency metric
  void Aggregate(AbstractMetricOld &source);

  // Returns a string representation of this latency metric
  const std::string GetInfo() const;

  // Returns a copy of the latencies collected
  CircularBuffer<double> Copy();

 private:
  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Circular buffer with capacity N that stores the <= N
  // most recent latencies collected
  CircularBuffer<double> latencies_;

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
