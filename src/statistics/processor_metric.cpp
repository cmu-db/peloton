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

#ifndef RUSAGE_THREAD

#include <mach/mach_init.h>
#include <mach/thread_act.h>
#include <mach/mach_port.h>

#endif

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
#ifdef RUSAGE_THREAD  // RUSAGE_THREAD is Linux-specific.
  struct rusage usage;
  int ret = getrusage(RUSAGE_THREAD, &usage);
   if (ret != 0) {
    throw StatException("Error getting resource usage");
  }
  user_time = GetMilliSec(usage.ru_utime);
  system_time = GetMilliSec(usage.ru_stime);
#else // https://stackoverflow.com/questions/13893134/get-current-pthread-cpu-usage-mac-os-x
  mach_port_t thread = mach_thread_self();
  thread_basic_info_data_t info;
  mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;
  kern_return_t kr = thread_info(thread, THREAD_BASIC_INFO, (thread_info_t) &info, &count);

  if (kr == KERN_SUCCESS && (info.flags & TH_FLAGS_IDLE) == 0) {
    user_time = ((double) info.user_time.microseconds) / 1000;
    system_time = ((double) info.system_time.microseconds) / 1000;
  }
  else {
    throw StatException("Error getting resource usage");
  }
  mach_port_deallocate(mach_task_self(), thread);
#endif

}

const std::string ProcessorMetric::GetInfo() const {
  std::stringstream ss;
  ss << "Query CPU Time (ms): [ ";
  ss << "system time=" << GetSystemDuration();
  ss << ", user time=" << GetUserDuration();
  ss << " ]";
  return ss.str();
}

void ProcessorMetric::Aggregate(AbstractMetric &source UNUSED_ATTRIBUTE) {}

}  // namespace stats
}  // namespace peloton
