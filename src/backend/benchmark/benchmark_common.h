//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// ycsb_workload.h
//
// Identification: src/backend/benchmark/ycsb_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace peloton {
namespace benchmark {

/////////////////////////////
///// Random Generator //////
/////////////////////////////

// Fast random number generator
class fast_random {
 public:
  fast_random(unsigned long seed) : seed(0) { set_seed0(seed); }

  inline unsigned long next() {
    return ((unsigned long)next(32) << 32) + next(32);
  }

  inline uint32_t next_u32() { return next(32); }

  inline uint16_t next_u16() { return (uint16_t)next(16); }

  /** [0.0, 1.0) */
  inline double next_uniform() {
    return (((unsigned long)next(26) << 27) + next(27)) / (double)(1L << 53);
  }

  inline char next_char() { return next(8) % 256; }

  inline char next_readable_char() {
    static const char readables[] =
        "0123456789@ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
    return readables[next(6)];
  }

  inline std::string next_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_char();
    return s;
  }

  inline std::string next_readable_string(size_t len) {
    std::string s(len, 0);
    for (size_t i = 0; i < len; i++) s[i] = next_readable_char();
    return s;
  }

  inline unsigned long get_seed() { return seed; }

  inline void set_seed(unsigned long seed) { this->seed = seed; }

 private:
  inline void set_seed0(unsigned long seed) {
    this->seed = (seed ^ 0x5DEECE66DL) & ((1L << 48) - 1);
  }

  inline unsigned long next(unsigned int bits) {
    seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
    return (unsigned long)(seed >> (48 - bits));
  }

  unsigned long seed;
};

class ZipfDistribution {
 public:
  ZipfDistribution(const uint64_t &n, const double &theta)
      : rand_generator(rand()) {
    // range: 1-n
    the_n = n;
    zipf_theta = theta;
    zeta_2_theta = zeta(2, zipf_theta);
    denom = zeta(the_n, zipf_theta);
  }
  double zeta(uint64_t n, double theta) {
    double sum = 0;
    for (uint64_t i = 1; i <= n; i++) sum += pow(1.0 / i, theta);
    return sum;
  }
  int GenerateInteger(const int &min, const int &max) {
    return rand_generator.next() % (max - min + 1) + min;
  }
  uint64_t GetNextNumber() {
    double alpha = 1 / (1 - zipf_theta);
    double zetan = denom;
    double eta =
        (1 - pow(2.0 / the_n, 1 - zipf_theta)) / (1 - zeta_2_theta / zetan);
    double u = (double)(GenerateInteger(1, 10000000) % 10000000) / 10000000;
    double uz = u * zetan;
    if (uz < 1) return 1;
    if (uz < 1 + pow(0.5, zipf_theta)) return 2;
    return 1 + (uint64_t)(the_n * pow(eta * u - eta + 1, alpha));
  }

  uint64_t the_n;
  double zipf_theta;
  double denom;
  double zeta_2_theta;
  fast_random rand_generator;
};

}  // namespace benchmark
}  // namespace peloton
