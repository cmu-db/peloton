//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.cpp
//
// Identification: src/backend/statistics/latency_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <mutex>

#include "backend/statistics/latency_metric.h"

namespace peloton {
namespace stats {

LatencyMetric::LatencyMetric(MetricType type, size_t max_history)
    : AbstractMetric(type) {
  max_history_ = max_history;
  latencies_.set_capacity(max_history_);
}

void LatencyMetric::Aggregate(AbstractMetric& source) {
  assert(source.GetType() == LATENCY_METRIC);

  LatencyMetric& latency_metric = static_cast<LatencyMetric&>(source);
  boost::circular_buffer<double> source_latencies = latency_metric.Copy();
  {
    // This method should only ever be called by the aggregator which
    // is the only thread to access its own latencies_, but we lock
    // here just to be safe. Either way the aggregator should never
    // have to block here.
    std::lock_guard<std::mutex> lock(latency_mutex_);
    for (double latency_value : source_latencies) {
      latencies_.push_back(latency_value);
    }
  }
}

boost::circular_buffer<double> LatencyMetric::Copy() {
  boost::circular_buffer<double> new_buffer;
  {
    // This method is only called by the aggregator to make
    // a copy of a worker thread's latencies_.
    std::lock_guard<std::mutex> lock(latency_mutex_);
    new_buffer = latencies_;
  }
  return new_buffer;
}

std::string LatencyMetric::ToString() {
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

LatencyMeasurements LatencyMetric::ComputeLatencies() {
  LatencyMeasurements measurements;
  if (latencies_.empty()) {
    return measurements;
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
  measurements.average_ = latency_sum / latencies_size;

  // Min, max, median, and percentiles are values at indexes
  measurements.min_ = sorted_latencies[0];
  measurements.max_ = sorted_latencies[latencies_size - 1];
  size_t mid = latencies_size / 2;
  if (latencies_size % 2 == 0 || latencies_size == 1) {
    measurements.median_ = sorted_latencies[mid];
  } else {
    measurements.median_ =
        (sorted_latencies[mid - 1] + sorted_latencies[mid]) / 2;
  }
  size_t index_25th = (size_t)(0.25 * latencies_size);
  size_t index_75th = (size_t)(0.75 * latencies_size);
  size_t index_99th = (size_t)(0.99 * latencies_size);
  measurements.perc_25th_ = sorted_latencies[index_25th];
  measurements.perc_75th_ = sorted_latencies[index_75th];
  measurements.perc_99th_ = sorted_latencies[index_99th];

  return measurements;
}

}  // namespace stats
}  // namespace peloton
