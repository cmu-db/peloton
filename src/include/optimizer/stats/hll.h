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

/*
 * A wrapper for libcount::HLL with murmurhash3
 */
class HLL {
 public:

  HLL(const int kPrecision = 8) {
    hll_ = libcount::HLL::Create(kPrecision);
  }

  void Update(type::Value& value) {
    hll_->Update(Hash(value));
  }

  uint64_t EstimateCardinality() {
    uint64_t cardinality = hll_->Estimate();
    LOG_INFO("Estimated cardinality with HLL: [%lu]", cardinality);
    return cardinality;
  }

  ~HLL() {
    delete hll_;
  }

 private:
  libcount::HLL* hll_;

  uint64_t Hash(type::Value& value) {
    uint64_t hash[2];
    const char* raw_value = value.ToString().c_str();
    MurmurHash3_x64_128(raw_value, (uint64_t)strlen(raw_value), 0, hash);
    return hash[0];
  }
};

} /* namespace optimizer */
} /* namespace peloton */
