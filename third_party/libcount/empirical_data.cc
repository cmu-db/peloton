//
// Copyright 2015 The libcount Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License. See the AUTHORS file for names of
// contributors.

#include "empirical_data.h"
#include <assert.h>
#include <stdio.h>
#include <algorithm>
#include <vector>
#include "hll_data.h"
#include "hll_limits.h"
#include "utility.h"

namespace libcount {

double EmpiricalAlpha(int precision) {
  assert(precision >= HLL_MIN_PRECISION);
  assert(precision <= HLL_MAX_PRECISION);
  switch (precision) {
    case 4:
      return 0.673;
    case 5:
      return 0.697;
    case 6:
      return 0.709;
    default:
      return (0.7213 / (1.0 + (1.079 / static_cast<double>(1 << precision))));
  }
}

double EmpiricalThreshold(int precision) {
  assert(precision >= HLL_MIN_PRECISION);
  assert(precision <= HLL_MAX_PRECISION);
  if ((precision < HLL_MIN_PRECISION) || (precision > HLL_MAX_PRECISION)) {
    return 0.0;
  }
  return THRESHOLD_DATA[precision - HLL_MIN_PRECISION];
}

double EmpiricalBias(double raw_estimate, int precision) {
  assert(precision >= HLL_MIN_PRECISION);
  assert(precision <= HLL_MAX_PRECISION);
  if ((precision < HLL_MIN_PRECISION) || (precision > HLL_MAX_PRECISION)) {
    return 0.0;
  }

  // There are separate raw estimate range and bias tables for each precision
  // level. The table starts at precision 4, which is at index 0.
  const int index = (precision - HLL_MIN_PRECISION);

  // Make aliases for the estimate, bias arrays we're interested in.
  const double* const estimates = ESTIMATE_DATA[index];
  const double* const biases = BIAS_DATA[index];

  // There up to 201 data points in each table of raw estimates, but the
  // number of points varies depending on the precision. Determine the
  // actual number of valid entries in the table.
  const int kMaxEntries = 201;
  const int kNumValidEntries = ValidTableEntries(estimates, kMaxEntries);

  // The raw estimate tables are sorted in ascending order. Search for the
  // pair of values in the table that straddle the input, 'raw_estimate'. We
  // do this by searching (linearly) for the first value in the estimate
  // table that is greater than 'raw_estimate'. We consider this to be the
  // "right-hand-side" of the pair that straddle the value.
  int rhs = 0;
  for (; rhs < kNumValidEntries; ++rhs) {
    if (estimates[rhs] > raw_estimate) {
      break;
    }
  }

  // Two boundary cases exist: if we exit the loop above and rhs is equal to
  // zero OR kNumValidEntries, then we'll return the first OR last element
  // of the bias table, respectively.
  if (rhs == 0) {
    return biases[0];
  } else if (rhs == kNumValidEntries) {
    return biases[kNumValidEntries - 1];
  }

  // Use linear interpolation to find a bias value.

  // Get values of left, right straddling table entries.
  const double left_neighbor = estimates[rhs - 1];
  const double right_neighbor = estimates[rhs];

  // Compute range (difference between entries) and scale factor.
  const double range = right_neighbor - left_neighbor;
  const double scale = (raw_estimate - left_neighbor) / range;

  // Get values of corresponding left, right bias values.
  const double left_bias = biases[rhs - 1];
  const double right_bias = biases[rhs];

  // Compute range of bias values and find interpolated value using
  // the scale factor computed above.
  const double bias_range = right_bias - left_bias;
  const double interpolated_bias = (scale * bias_range) + left_bias;

  // Sanity Check the interpolated bias. We assert in debug mode, and abort
  // in release mode. This seems extreme, but recognize that if the value
  // we interpolated above does not lie between the left, right entries in
  // the bias table that this reflects a programmer error in the above
  // code that intends to interpolate a value.
  if (left_bias < right_bias) {
    assert(interpolated_bias >= left_bias);
    assert(interpolated_bias <= right_bias);
    if ((interpolated_bias < left_bias) || (interpolated_bias > right_bias)) {
      abort();
    }
  } else {
    assert(interpolated_bias <= left_bias);
    assert(interpolated_bias >= right_bias);
    if ((interpolated_bias > left_bias) || (interpolated_bias < right_bias)) {
      abort();
    }
  }

  return interpolated_bias;
}

// The empirical bias tables contain varying numbers of entries, depending
// on the precision level. Unused table entries (occuring at the end of the
// arrays) are zero filled. This function returns the actual number of
// valid elements in such an array.
int ValidTableEntries(const double* array, int size) {
  const double EPSILON = 0.0001;
  for (int i = 0; i < size; ++i) {
    if (libcount::IsDoubleEqual(array[i], 0.0, EPSILON)) {
      return i;
    }
  }
  return size;
}

}  // namespace libcount
