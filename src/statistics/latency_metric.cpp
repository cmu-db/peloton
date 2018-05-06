//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// latency_metric.cpp
//
// Identification: src/statistics/latency_metric.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "statistics/latency_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

LatencyMeasurements LatencyMetricRawData::DescriptiveFromRaw() {
  LatencyMeasurements measurements;
  if (latencies_.IsEmpty()) {
    return measurements;
  }
  std::vector<double> sorted_latencies;
  double latency_sum = 0.0;
  {
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
