#pragma once

#include <cmath>
#include <vector>
#include <cassert>

#include <murmur3/MurmurHash3.h>

#include "optimizer/stats/distinct_value_counter.h"

namespace peloton {
namespace optimizer {

/*
 * HyperLogLog - an algorithm estimates cardinality
 * Paper: http://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf
 * Impl is based on page 140 Fig 3.
 * Use MurmurHash32 now.
 * TODO: change it to 64 bits, also modify MurmurHash3 third party lib
 * Note: it seems that the algorithm in the original paper has many problems.
 *       Need to measure its performance.
 * Only support strings for now.
 *
 * We also wanna try the improved version HyperLogLog++ but it's complicated...
 * Paper: https://research.google.com/pubs/pub40671.html
 */
class HyperLogLog : public DistinctValueCounter {
 public:
  using Uint_t = uint32_t;

  uint8_t b; // Register bit width
  Uint_t m; // Register size
  std::vector<uint8_t> M; // Register set
  double alphaMM; // alphaM * m * m

  static constexpr uint8_t HASH_SIZE = 32;
  static constexpr double POW_2_32 = 4294967296.0;
  static constexpr double NEG_POW_2_32 = -4294967296.0;

  /* Count leading zeros */
  inline uint8_t CLZ(Uint_t x, uint8_t b) { return std::min(b, (uint8_t) ::__builtin_clz(x)); }

  double AlphaM(const int m) {
    switch (m) {
      case 16:
        return 0.673;
      case 32:
        return 0.697;
      case 64:
        return 0.709;
      default:
        return 0.7213 / (1.0 + 1.079 / m);
    }
  }

  /*
   * b in [4, 16]
   */
  HyperLogLog(uint8_t b = 4)
    : DistinctValueCounter{},
      b{b},
      m(1 << b),
      M(m, 0),
      alphaMM{AlphaM(m) * m * m} {}

  // TODO: Handle NULL value properly!
  void AddValue(type::Value &value) {
    if (value.GetTypeId() == type::Type::TypeId::INVALID) {
      return;
    }
    Add(value.Hash());
  }

  void Add(Uint_t hash) {
    Uint_t j = hash >> (HASH_SIZE - b);
    // Get number of of leading zero + 1 (the position of the leftmost 1-bit)
    uint8_t r = CLZ((hash << b), HASH_SIZE - b) + 1;
    if (r > M[j]) {
      M[j] = r;
    }
  }

  // MurmurHash does not work properly. Deprecated.
  void Add(const char* item, uint32_t count = 1) {
    // using 32 bit MurmurHash for now
    Uint_t hash = MurmurHash3_x86_32(item, count, 0);
    // LOG_INFO("hash is %u", hash);
    Uint_t j = hash >> (HASH_SIZE - b);
    // Get number of of leading zero + 1 (the position of the leftmost 1-bit)
    uint8_t r = CLZ((hash << b), HASH_SIZE - b) + 1;
    if (r > M[j]) {
      M[j] = r;
    }
  }

  double EstimateCardinality() {
    double E;
    double sum = 0.0;
    for (Uint_t i = 0; i < m; i++) {
      sum += 1.0 / (1 << M[i]);
    }
    E = alphaMM / sum;
    if (E <= 2.5 * m) {
      // V is the number of registers equal to 0
      Uint_t V = 0;
      for (Uint_t i = 0; i < m; i++) {
        V += (M[i] == 0);
      }
      if (V != 0) {
        return m * log(1.0 * m / V);
      }
    } else if (E > (1.0 / 30) * POW_2_32) {
      return NEG_POW_2_32 * log(1 - E / POW_2_32);
    }

    return E;
  }

  // http://stackoverflow.com/questions/8848575/fastest-way-to-reset-every-value-of-stdvectorint-to-0
  void Clear() {
    std::fill(M.begin(), M.end(), 0);
  }
};

} /* namespace optimizer */
} /* namespace peloton */
