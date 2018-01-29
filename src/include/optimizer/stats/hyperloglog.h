//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyperloglog.h
//
// Identification: src/include/optimizer/stats/hyperloglog.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cmath>
#include <vector>

#include <libcount/hll.h>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"
#include "optimizer/stats/stats_util.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// HyperLogLog
//===--------------------------------------------------------------------===//
class HyperLogLog {
 public:
  HyperLogLog(const int precision = 8)
      : precision_{precision}, register_count_{1 << precision} {
    hll_ = libcount::HLL::Create(precision);
  }

  ~HyperLogLog() { delete hll_; }

  void Update(const type::Value& value) {
    hll_->Update(StatsUtil::HashValue(value));
  }

  uint64_t EstimateCardinality() {
    uint64_t cardinality = hll_->Estimate();
    LOG_TRACE("Estimated cardinality: %" PRId64, cardinality);
    return cardinality;
  }

  // Estimate relative error for HLL.
  inline double RelativeError() {
    PELOTON_ASSERT(register_count_ > 0);
    return 1.04 / std::sqrt(register_count_);
  }

 private:
  const int precision_;
  const int register_count_;
  libcount::HLL* hll_;
};

}  // namespace optimizer
}  // namespace peloton
