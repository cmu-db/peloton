//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// processor_metric.cpp
//
// Identification: src/statistics/processor_metric.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/processor_metric.h"

namespace peloton {
namespace stats {

ProcessorMetric::ProcessorMetric(MetricType type) : AbstractMetric(type) {}

void ProcessorMetric::StartTimer() {
  UpdateTimeInt(user_time_begin_, sys_time_begin_);
}

void ProcessorMetric::RecordTime() {
  UpdateTimeInt(user_time_end_, sys_time_end_);
}

double ProcessorMetric::GetMilliSec(struct timeval time) const {
  return time.tv_sec * 1000 + time.tv_usec / 1000.0;
}

void ProcessorMetric::UpdateTimeInt(double &user_time, double &system_time) {
  struct rusage usage;
  int ret = getrusage(RUSAGE_THREAD, &usage);
  if (ret != 0) {
    throw StatException("Error getting resource usage");
  }
  user_time = GetMilliSec(usage.ru_utime);
  system_time = GetMilliSec(usage.ru_stime);
}

const std::string ProcessorMetric::GetInfo() const {
  std::stringstream ss;
  ss << "Query CPU Time (ms): [ ";
  ss << "system time=" << GetSystemDuration();
  ss << ", user time=" << GetUserDuration();
  ss << " ]" << std::endl;
  return ss.str();
}

void ProcessorMetric::Aggregate(AbstractMetric &source UNUSED_ATTRIBUTE) {}

}  // namespace stats
}  // namespace peloton
