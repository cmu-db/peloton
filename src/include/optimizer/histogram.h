#pragma once

#include <algorithm>
#include <vector>
#include <iterator>
#include <cstdint>
#include <string>
#include <iostream>
#include <cmath>
#include <cassert>
#include <cfloat>

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
 * - Add percentile calculation (may not be useful??)
 * - Add thresholds to keep histogram bound constant
 */
class Histogram {
 public:

  class Bin;

  using Height = double;    // use double to avoid type conversion during computation

  const uint8_t max_bins;   // For performance it shouldn't be greater than 255
  std::vector<Bin> bins;
  Height total_count;
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

  void InsertBin(Bin &bin) {
    // CheckBin(bin);

    total_count += bin.height;
    if (bin.center < minimum) {
      minimum = bin.center;
    }
    if (bin.center > maximum) {
      maximum = bin.center;
    }

    int index = binary_search(bins, 0, bins.size() - 1, bin);

    if (index >= 0) {
      bins[index].height += bin.height;
    } else {
      index = abs(index) - 1;
      bins.insert(bins.begin() + index, bin);
    }
  }

  // Merge n + 1 number of bins to n bins based on update algorithm.
  void MergeTwoBinsWithMinGap() {
    int min_gap_idx = -1;
    double min_gap = DBL_MAX;
    for (uint8_t i = 0; i < bins.size() - 1; i++) {
      double gap = abs(bins[i].center - bins[i + 1].center);
      if (gap < min_gap) {
        min_gap = gap;
        min_gap_idx = i;
      }
    }
    Bin prev_bin = bins[min_gap_idx];
    Bin next_bin = bins[min_gap_idx + 1];
    prev_bin.MergeWith(next_bin);
    bins.erase(bins.begin() + min_gap_idx + 1);
  }

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
   * Input: a point p such that p1 < b < pB
   *
   * Output: estimated number of points in the interval [-Inf, b]
   */
  // double sum(Point p) {
  //   CheckBoundry(b);
  //
  //
  // }

 /*
  * Input: an integer num_bins.
  *
  * Output: B boundry points with the property that the number of
  *         points between two consecutive numbers uj, uj+1 and
  *         the number of data points to the left of u1 and to the
  *         right of uB is equal to sum of all points / B.
  */
  // std::vector<Point> uniform(uint8_t num_bins) {
  //
  // }


  // void CheckBin(const Bin &bin) {
  //   void(bin);
  // }

  // Utility function to visualize histogram
  // std::string Print() {
  //   std::ostringstream os;
  //   os << "\n"
  //   os << "---[Histogram]---\n";
  //   os << "Num Bin: " << bins.size();
  //   os << "\n";
  //   return os.str();
  // }

  // A more useful binary search based on Java's Collections.binarySearch
  // It returns -index - 1 if not found.
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

 /*
  * Represent a bin of a historgram.
  *
  * center refers to x-
  * heighth refers to frequency
  */
  class Bin {
   public:

    double center;
    Height height;

    Bin(double p, Height height) :
     center{p},
     height{height}
    {}

    void MergeWith(Bin &bin) {
      Height new_height = height + bin.height;
      center = (center * height + bin.center * bin.height) / new_height;
    }

    inline bool operator <(const Bin &bin) const {
      return center < bin.center;
    }

    // TODO: check double comparison!
    inline bool operator ==(const Bin &bin) const {
      return center == bin.center;
    }

    inline bool operator >(const Bin &bin) const {
      return center > bin.center;
    }

    inline bool operator !=(const Bin &bin) const {
      return center != bin.center;
    }
  };
};

} /* namespace optimizer */
} /* namespace peloton */
