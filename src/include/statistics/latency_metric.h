//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.h
//
// Identification: src/include/statistics/latency_metric.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
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

  // Computes descriptive statistics on the aggregated latencies,
  // then writes these computed values to the catalog.
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

  // Circular buffer with capacity N that stores up to the N
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

}  // namespace stats
}  // namespace peloton
