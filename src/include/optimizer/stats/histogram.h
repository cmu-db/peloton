//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// histogram.h
//
// Identification: src/include/optimizer/stats/histogram.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <cassert>
#include <cfloat>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <string>
#include <vector>
#include <tuple>

#include "type/value.h"
#include "type/type.h"
#include "common/macros.h"
#include "common/logger.h"

namespace peloton {
namespace optimizer {

//===--------------------------------------------------------------------===//
// Histogram
//===--------------------------------------------------------------------===//

/*
 * Online histogram implementation based on A Streaming Parallel Decision Tree
 * Algorithm (http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf)
 * Specifically Algorithm 1, 3, and 4.
 */
class Histogram {
 public:
  /*
   * Constructor.
   *
   * max_bins - maximum number of bins in histogram.
   */
  Histogram(const uint8_t max_bins = 100)
      : max_bins_{max_bins},
        bins{},
        total_{0},
        minimum_{DBL_MAX},
        maximum_{DBL_MIN} {}

  /*
   * Input: A point p
   *
   * Update the histogram that represents the set S U {p},
   * where S is the set represented by h. Keep bin number unchanged.
   */
  void Update(double p) {
    Bin bin{p, 1};
    InsertBin(bin);
    if (bins.size() > max_bins_) {
      MergeTwoBinsWithMinGap();
    }
  }

  /*
   * Update histogram. It only supports numeric types.
   */
  void Update(const type::Value &value) {
    if (value.IsNull()) {
      LOG_TRACE("Histogram update value is null");
      return;
    }
    if (value.CheckInteger() || value.GetTypeId() == type::TypeId::TIMESTAMP) {
      int raw_value = value.GetAs<int>();
      Update(raw_value);
    } else if (value.GetTypeId() == type::TypeId::DECIMAL) {
      int raw_value = value.GetAs<double>();
      Update(raw_value);
    } else {
      LOG_TRACE("Unsupported histogram value type %s",
                TypeIdToString(value.GetTypeId()).c_str());
    }
  }

  /*
   * Input: a point b such that p1 < b < pB
   *
   * Output: estimated number of points in the interval [-Inf, b]
   */
  double Sum(double b) {
    if (bins.size() == 0) return 0.0;

    if (b >= bins.back().p) {
      return total_;
    } else if (b < bins.front().p) {
      return 0.0;
    }

    Bin bin{b, 1};
    int i = BinarySearch(bins, 0, bins.size() - 1, bin);
    if (i < 0) {
      // -1 because we want index to be element less than b
      i = std::abs(i + 1) - 1;
    }

    double pi, pi1, mi, mi1;
    std::tie(pi, pi1, mi, mi1) = GetInterval(bins, i);

    double mb = mi + (mi1 - mi) / (pi1 - pi) * (b - pi);

    double s = ((mi + mb) / 2.0) * ((b - pi) / (pi1 - pi));

    for (int j = 0; j < i; j++) {
      s += bins[j].m;
    }

    s = s + mi / 2;

    return s;
  }

  /*
   * Return boundry points at most size max_bins with the property that the
   * number of points between two consecutive numbers uj, uj+1 and the
   * number of data points to the left of u1 and to the right of uB is
   * equal to sum of all points / max_bins.
   */
  std::vector<double> Uniform() {
    PELOTON_ASSERT(max_bins_ > 0);

    std::vector<double> res{};
    if (bins.size() <= 1 || total_ <= 0) return res;

    uint8_t i = 0;
    for (uint8_t j = 0; j < bins.size() - 1; j++) {
      double s = (j * 1.0 + 1.0) / max_bins_ * total_;
      while (i < bins.size() - 1 && Sum(bins[i + 1].p) < s) {
        i += 1;
      }
      PELOTON_ASSERT(i < bins.size() - 1);
      double pi, pi1, mi, mi1;
      std::tie(pi, pi1, mi, mi1) = GetInterval(bins, i);

      double d = s - Sum(bins[i].p);
      double a = mi1 - mi;
      double b = 2.0 * mi;
      double c = -2.0 * d;
      double z =
          a != 0 ? (-b + std::sqrt(b * b - 4.0 * a * c)) / (2.0 * a) : -c / b;
      double uj = pi + (pi1 - pi) * z;
      res.push_back(uj);
    }
    return res;
  }

  inline double GetMaxValue() { return maximum_; }

  inline double GetMinValue() { return minimum_; }

  inline uint64_t GetTotalValueCount() { return std::floor(total_); }

  inline uint8_t GetMaxBinSize() { return max_bins_; }

 private:
  class Bin;

  const uint8_t max_bins_;
  std::vector<Bin> bins;
  double total_;
  double minimum_;
  double maximum_;

  void InsertBin(const Bin &bin) {
    total_ += bin.m;
    if (bin.p < minimum_) {
      minimum_ = bin.p;
    }
    if (bin.p > maximum_) {
      maximum_ = bin.p;
    }

    int index = BinarySearch(bins, 0, bins.size() - 1, bin);

    if (index >= 0) {
      bins[index].m += bin.m;
    } else {
      index = std::abs(index) - 1;
      bins.insert(bins.begin() + index, bin);
    }
  }

  /* Merge n + 1 number of bins to n bins based on update algorithm. */
  void MergeTwoBinsWithMinGap() {
    int min_gap_idx = -1;
    double min_gap = DBL_MAX;
    for (uint8_t i = 0; i < bins.size() - 1; i++) {
      double gap = std::abs(bins[i].p - bins[i + 1].p);
      if (gap < min_gap) {
        min_gap = gap;
        min_gap_idx = i;
      }
    }
    //		PELOTON_ASSERT(min_gap_idx >= 0 && min_gap_idx < bins.size());
    Bin &prev_bin = bins[min_gap_idx];
    Bin &next_bin = bins[min_gap_idx + 1];
    prev_bin.MergeWith(next_bin);
    bins.erase(bins.begin() + min_gap_idx + 1);
  }

  /*
   * A more useful binary search based on Java's Collections.binarySearch
   * It returns -index - 1 if not found, where index is position for insertion:
   * the first index with element **greater** than the key.
   */
  template <typename T>
  int BinarySearch(const std::vector<T> &vec, int start, int end,
                   const T &key) {
    if (start > end) {
      return -(start + 1);
    }

    const int middle = start + ((end - start) / 2);

    if (vec[middle] == key) {
      return middle;
    } else if (vec[middle] > key) {
      return BinarySearch(vec, start, middle - 1, key);
    }
    return BinarySearch(vec, middle + 1, end, key);
  }

  inline std::tuple<double, double, double, double> GetInterval(
      std::vector<Bin> bins, uint8_t i) {
    PELOTON_ASSERT(i < bins.size() - 1);
    return std::make_tuple(bins[i].p, bins[i + 1].p, bins[i].m, bins[i + 1].m);
  }

  inline void PrintHistogram() {
    LOG_INFO("Histogram: total=[%f] num_bins=[%lu]", total_, bins.size());
    for (Bin b : bins) {
      b.Print();
    }
  }

  void PrintUniform(const std::vector<double> &vec) {
    std::string output{"{"};
    for (uint8_t i = 0; i < vec.size(); i++) {
      output += std::to_string(vec[i]);
      output += (i == vec.size() - 1 ? "}" : ", ");
    }
    LOG_INFO("%s", output.c_str());
  }

  class Bin {
   public:
    double p;
    double m;

    Bin(double p, double m) : p{p}, m{m} {}

    void MergeWith(const Bin &bin) {
      double new_m = m + bin.m;
      p = (p * m + bin.p * bin.m) / new_m;
      m = new_m;
    }

    inline bool operator<(const Bin &bin) const { return p < bin.p; }

    inline bool operator==(const Bin &bin) const { return p == bin.p; }

    inline bool operator>(const Bin &bin) const { return p > bin.p; }

    inline bool operator!=(const Bin &bin) const { return p != bin.p; }

    void Print() { LOG_INFO("Bin: p=[%f],m=[%f]", p, m); }
  };
};

}  // namespace optimizer
}  // namespace peloton
