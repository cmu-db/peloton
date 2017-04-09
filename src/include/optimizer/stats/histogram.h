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

#include "type/value.h"
#include "type/type.h"
#include "common/macros.h"
#include "common/logger.h"

namespace peloton {
namespace optimizer {

/*
 * Online histogram implementation based on
 * http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 * Specifically Algorithm 1, 3, and 4
 *
 * One problem associated with this appraoch is that
 * int64_t type convert to double might fail, and
 * calculation may overflow...
 *
 * Note: histogram only applys to data that's comparable.
 *
 * TOOD:
 * - move helper functions to private
 * - Add mean, variance,
 * - Add thresholds to keep histogram bound constant
 */
class Histogram {
 public:

  class Bin;

  const uint8_t max_bins;   // For performance it shouldn't be greater than 255
  std::vector<Bin> bins;
  double total_count;
  double minimum;
  double maximum;

  /*
   * Constructor.
   *
   * max_bins - maximum number of bins in histogram.
   */
  Histogram(uint8_t max_bins = 10)
  :
   max_bins{max_bins},
   bins{},
   total_count{0},
   minimum{DBL_MAX},
   maximum{DBL_MIN}
  {}

  /*
   * Input: A point p
   *
   * Update the histogram that represents the set S U {p},
   * where S is the set represented by h. Keep bin number unchanged.
   */
   void Update(double p) {
     Bin bin{p, 1};
     InsertBin(bin);
     if (bins.size() > max_bins) {
       MergeTwoBinsWithMinGap();
     }
   }

   /*
    * Peloton type adapter
    */
   void Update(type::Value& value) {
     PL_ASSERT(value.CheckInteger() || value.GetTypeId() == type::Type::DECIMAL);
     double raw_value = atof(value.ToString().c_str());
     Update(raw_value);
   }

   /*
    * Input: a point b such that p1 < b < pB
    *
    * Output: estimated number of points in the interval [-Inf, b]
    */
  double Sum(double b) {
    // TODO: should handle those cases correctly
    assert(bins.size() >= 2);
    assert(b > bins[0].p);
    assert(b < bins[bins.size()].p);

    Bin bin{b, 1};
    int i = binary_search(bins, 0, bins.size() - 1, bin);
    if (i < 0) {
      // -1 because we want index to be element less than b
      i = std::abs(i + 1) - 1;
    }
    assert(i >= 0 && i < bins.size() - 1);

    Bin b_i = bins[i];
    Bin b_i1 = bins[i + 1];
    double m_i = b_i.m;
    double p_i = b_i.p;
    double m_i1 = b_i1.m;
    double p_i1 = b_i1.p;

    double m_b = m_i + (m_i1 - m_i) / (p_i1 - p_i) * (b - p_i);

    double s = ((m_i + m_b) / 2 ) * ((b - p_i) / (p_i1 - p_i));

    for (int j = 0; j < i; j++) {
      s += bins[j].m;
    }

    s = s + m_i / 2;

    return s;
  }

 /*
  * Input: an integer B, corresponding to B_tilde in paper.
  *
  * Output: B boundry points with the property that the number of
  *         points between two consecutive numbers uj, uj+1 and
  *         the number of data points to the left of u1 and to the
  *         right of uB is equal to sum of all points / B.
  */
  std::vector<double> Uniform(uint8_t B) {
    assert(B != 0);
    // check bins.size() <= 1 return empty
    std::vector<double> res{};
    double gap = total_count / B;
    int i = 0;
    double sum_i = 0;
    double sum_i1 = bins[0].m;
    //double sum_i1 = 0;
    if (total_count > 0) {
      for (uint8_t j = 1; j < B; j++) {
        double s = j * gap;
        // printf("s=%f\n", s);

        while (sum_i1 < s) {
          sum_i  = sum_i1;
          sum_i1 += bins[i].m;
          i += 1;
        }
        double u_j;

        if (i == 0) { // TODO: I come up with this solution....it may break the algorithm
          u_j = s;
        } else {
          Bin b_i = bins[i - 1];
          Bin b_i1 = bins[i];
          double p_i = b_i.p;
          double p_i1 = b_i1.p;
          double m_i = b_i.m;
          double m_i1 = b_i1.m;

          double a, b, c, d, z;
          d = s - sum_i;
          a = m_i1 - m_i;
          b = 2 * m_i;
          c = -2 * d;
          if (a == 0) {
            z = -c / b;
          } else {
            z = (-b + std::sqrt(b * b - 4 * a * c)) / (2 * a);
          }
          u_j = p_i + (p_i1 - p_i) * z;
        }
        res.push_back(u_j);
      }
    }
    PrintUniform(res);
    return res;
  }

  // Utility function to visualize histogram
  void Print() {
    printf("Histogram: total_count=[%f] num_bins=[%lu]\n", total_count, bins.size());
    for (Bin b : bins) {
      b.Print();
    }
    printf("\n");
  }

  void PrintUniform(const std::vector<double> &vec) {
    printf("Printing uniform histogram...\n");
    for (double x : vec) {
      printf("[%f]", x);
    }
    printf("\n");
  }

 private:

  void InsertBin(Bin &bin) {
    // CheckBin(bin);

    total_count += bin.m;
    if (bin.p < minimum) {
      minimum = bin.p;
    }
    if (bin.p > maximum) {
      maximum = bin.p;
    }

    int index = binary_search(bins, 0, bins.size() - 1, bin);

    if (index >= 0) {
      bins[index].m += bin.m;
    } else {
      index = std::abs(index) - 1;
      bins.insert(bins.begin() + index, bin);
    }
  }

  // Merge n + 1 number of bins to n bins based on update algorithm.
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
    assert(min_gap_idx >= 0 && min_gap_idx < bins.size());
    Bin &prev_bin = bins[min_gap_idx];
    Bin &next_bin = bins[min_gap_idx + 1];
    prev_bin.MergeWith(next_bin);
    bins.erase(bins.begin() + min_gap_idx + 1);
  }

  // A more useful binary search based on Java's Collections.binarySearch
  // It returns -index - 1 if not found, where index is position for insertion:
  // the first index with element **greater** than the key.
  template <typename T>
  int binary_search(const std::vector<T> &vec, int start, int end, const T &key) {
    if (start > end) {
        return -(start + 1);
    }

    const int middle = start + ((end - start) / 2);

    if (vec[middle] == key) {
        return middle;
    } else if (vec[middle] > key) {
        return binary_search(vec, start, middle - 1, key);
    }
    return binary_search(vec, middle + 1, end, key);
  }

 public:

 /*
  * Represent a bin of a historgram.
  */
  class Bin {
   public:

    double p;
    double m;

    Bin(double p, double m) :
     p{p},
     m{m}
    {}

    void MergeWith(Bin &bin) {
      double new_m = m + bin.m;
      p = (p * m + bin.p * bin.m) / new_m;
      m = new_m;
    }

    inline bool operator <(const Bin &bin) const {
      return p < bin.p;
    }

    inline bool operator ==(const Bin &bin) const {
      return p == bin.p;
    }

    inline bool operator >(const Bin &bin) const {
      return p > bin.p;
    }

    inline bool operator !=(const Bin &bin) const {
      return p != bin.p;
    }

    void Print() {
      printf("Bin: p=[%f],m=[%f]\n", p, m);
    }
  };
};

} /* namespace optimizer */
} /* namespace peloton */
