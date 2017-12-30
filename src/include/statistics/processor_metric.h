//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// processor_metric.h
//
// Identification: src/statistics/processor_metric.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <sstream>

#include "common/internal_types.h"
#include "common/exception.h"
#include "common/macros.h"

#include <sys/resource.h>
#include "statistics/abstract_metric.h"
#include "statistics/access_metric.h"

namespace peloton {
namespace stats {

/**
 * Metric for storing raw processor execution time values.
 */
class ProcessorMetric : public AbstractMetric {
 public:
  ProcessorMetric(MetricType type);

  // Reset the metric
  inline void Reset() {
    user_time_begin_ = 0;
    user_time_end_ = 0;
    sys_time_begin_ = 0;
    sys_time_end_ = 0;
  }

  // Starts the timer
  void StartTimer();

  // Stops the timer and records the total time of execution
  void RecordTime();

  // Get the CPU time for user execution (ms)
  inline double GetUserDuration() const {
    PL_ASSERT(user_time_end_ - user_time_begin_ >= 0);
    return user_time_end_ - user_time_begin_;
  }

  // Get the CPU time for system execution (ms)
  inline double GetSystemDuration() const {
    PL_ASSERT(sys_time_end_ - sys_time_begin_ >= 0);
    return sys_time_end_ - sys_time_begin_;
  }

  // Returns a string representation of this latency metric
  const std::string GetInfo() const;

  // Combines the source processor metric with this processor metric
  void Aggregate(AbstractMetric &source);

 private:
  //===--------------------------------------------------------------------===//
  // HELPER METHODS
  //===--------------------------------------------------------------------===//

  // Utility function to convert struc timeval to millisecond
  inline double GetMilliSec(struct timeval time) const;

  // Internal function to update CPU time values
  void UpdateTimeInt(double &user_time, double &system_time);

  //===--------------------------------------------------------------------===//
  // MEMBERS
  //===--------------------------------------------------------------------===//

  // Begin CPU time (ms) for user execution
  double user_time_begin_ = 0;

  // End CPU time (ms) for user execution
  double user_time_end_ = 0;

  // Begin CPU time (ms) for system execution
  double sys_time_begin_ = 0;

  // End CPU time (ms) for system execution
  double sys_time_end_ = 0;
};

}  // namespace stats
}  // namespace peloton
