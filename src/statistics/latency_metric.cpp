//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.cpp
//
// Identification: src/statistics/latency_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "statistics/latency_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

LatencyMetric::LatencyMetric(MetricType type, size_t max_history)
    : AbstractMetric(type) {
  max_history_ = max_history;
  latencies_.SetCapaciry(max_history_);
}

void LatencyMetric::Aggregate(AbstractMetric& source) {
  PL_ASSERT(source.GetType() == MetricType::LATENCY);

  LatencyMetric& latency_metric = static_cast<LatencyMetric&>(source);
  CircularBuffer<double> source_latencies = latency_metric.Copy();
  {
    // This method should only ever be called by the aggregator which
    // is the only thread to access its own latencies_, but we lock
    // here just to be safe. Either way the aggregator should never
    // have to block here.
    std::lock_guard<std::mutex> lock(latency_mutex_);
    for (double latency_value : source_latencies) {
      latencies_.PushBack(latency_value);
    }
  }
}

CircularBuffer<double> LatencyMetric::Copy() {
  CircularBuffer<double> new_buffer;
  {
    // This method is only called by the aggregator to make
    // a copy of a worker thread's latencies_.
    std::lock_guard<std::mutex> lock(latency_mutex_);
    new_buffer = latencies_;
  }
  return new_buffer;
}

const std::string LatencyMetric::GetInfo() const {
  std::stringstream ss;
  ss << "TXN LATENCY (ms): [ ";
  ss << "average=" << latency_measurements_.average_;
  ss << ", min=" << latency_measurements_.min_;
  ss << ", 25th-%-tile=" << latency_measurements_.perc_25th_;
  ss << ", median=" << latency_measurements_.median_;
  ss << ", 75th-%-tile=" << latency_measurements_.perc_75th_;
  ss << ", 99th-%-tile=" << latency_measurements_.perc_99th_;
  ss << ", max=" << latency_measurements_.max_;
  ss << " ]";
  return ss.str();
}

void LatencyMetric::ComputeLatencies() {
  // LatencyMeasurements measurements;
  if (latencies_.IsEmpty()) {
    return;
  }
  std::vector<double> sorted_latencies;
  double latency_sum = 0.0;
  {
    // This method is called only by the aggregator when
    // after it's aggregated all worker threads latencies
    // into its own latencies_ buffer. Still we lock here
    // just in case.
    std::lock_guard<std::mutex> lock(latency_mutex_);
    for (double latency : latencies_) {
      sorted_latencies.push_back(latency);
      latency_sum += latency;
    }
  }

  std::sort(sorted_latencies.begin(), sorted_latencies.end());
  size_t latencies_size = sorted_latencies.size();

  // Calculate average
  latency_measurements_.average_ = latency_sum / latencies_size;

  // Min, max, median, and percentiles are values at indexes
  latency_measurements_.min_ = sorted_latencies[0];
  latency_measurements_.max_ = sorted_latencies[latencies_size - 1];
  size_t mid = latencies_size / 2;
  if (latencies_size % 2 == 0 || latencies_size == 1) {
    latency_measurements_.median_ = sorted_latencies[mid];
  } else {
    latency_measurements_.median_ =
        (sorted_latencies[mid - 1] + sorted_latencies[mid]) / 2;
  }
  size_t index_25th = (size_t)(0.25 * latencies_size);
  size_t index_75th = (size_t)(0.75 * latencies_size);
  size_t index_99th = (size_t)(0.99 * latencies_size);
  latency_measurements_.perc_25th_ = sorted_latencies[index_25th];
  latency_measurements_.perc_75th_ = sorted_latencies[index_75th];
  latency_measurements_.perc_99th_ = sorted_latencies[index_99th];
}

}  // namespace stats
}  // namespace peloton
