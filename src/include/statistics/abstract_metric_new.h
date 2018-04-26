//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_metric_new.h
//
// Identification: src/statistics/abstract_metric_new.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>

#include "common/internal_types.h"
#include "common/printable.h"
#include "statistics/stat_insertion_point.h"

namespace peloton {
namespace stats {

/**
 * Abstract class for metrics
 * A metric should be able to:
 * (1) identify its type;
 * (2) print itself (ToString);
 * (3) reset itself;
 */
class AbstractMetricNew : public Printable {
 public:
  virtual ~AbstractMetricNew() = default;

  virtual void OnTransactionCommit() {};

  virtual void OnTransactionAbort() {};

  // TODO(tianyu): more handler methods

  virtual void CollectIntoCatalog() = 0;

};

}  // namespace stats
}  // namespace peloton
