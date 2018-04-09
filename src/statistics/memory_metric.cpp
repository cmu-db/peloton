//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// memory_metric.cpp
//
// Identification: src/statistics/memory_metric.cpp
//
// Copyright (c) 2015-18, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "statistics/memory_metric.h"
#include "common/macros.h"

namespace peloton {
namespace stats {

void MemoryMetric::Aggregate(AbstractMetric &source) {
  PELOTON_ASSERT(source.GetType() == MetricType::MEMORY);

  auto memory_metric = static_cast<MemoryMetric &>(source);
  alloc_.Aggregate(memory_metric.alloc_);
  usage_.Aggregate(memory_metric.usage_);
  }

}  // namespace stats
}  // namespace peloton
