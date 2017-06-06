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

#include <murmur3/MurmurHash3.h>
#include <libcount/hll.h>

#include "type/value.h"
#include "common/macros.h"
#include "common/logger.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// HyperLogLog
//===--------------------------------------------------------------------===//
class HyperLogLog {
 public:

  HyperLogLog(const int precision = 8)
  :
    precision_{precision},
    register_count_{1 << precision}
  {
    hll_ = libcount::HLL::Create(precision);
  }

  ~HyperLogLog() {
    delete hll_;
  }

  void Update(const type::Value& value) {
    hll_->Update(Hash(value));
  }

  uint64_t EstimateCardinality() {
    uint64_t cardinality = hll_->Estimate();
    LOG_TRACE("Estimated cardinality: %lu", cardinality);
    return cardinality;
  }

  // Estimate relative error for HLL.
  inline double RelativeError() {
    PL_ASSERT(register_count_ > 0);
    return 1.04 / std::sqrt(register_count_);
  }

 private:
  const int precision_;
  const int register_count_;
  libcount::HLL* hll_;

  // Compute hash for given value using Murmur3.
  uint64_t Hash(const type::Value& value) {
    uint64_t hash[2];
    if (value.CheckInteger())
    {
      int raw_value = value.GetAs<int>();
      MurmurHash3_x64_128(&raw_value, sizeof(raw_value), 0, hash);
    }
    else if (value.GetTypeId() == type::Type::VARCHAR ||
             value.GetTypeId() == type::Type::VARBINARY)
    {
      const char* raw_value = value.GetData();
      MurmurHash3_x64_128(raw_value, (uint64_t)strlen(raw_value), 0, hash);
    }
    else // Hack for other data types.
    {
      std::string value_str = value.ToString();
      const char* raw_value = value_str.c_str();
      MurmurHash3_x64_128(raw_value, (uint64_t)strlen(raw_value), 0, hash);
    }
    return hash[0];
  }
};

} /* namespace optimizer */
} /* namespace peloton */
