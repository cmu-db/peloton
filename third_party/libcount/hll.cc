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

#include "hll.h"
#include <assert.h>
#include <errno.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>
#include "empirical_data.h"
#include "utility.h"

namespace {

using libcount::CountLeadingZeroes;
using std::max;

// Helper that calculates cardinality according to LinearCounting
double LinearCounting(double register_count, double zeroed_registers) {
  return register_count * log(register_count / zeroed_registers);
}

// Helper to calculate the index into the table of registers from the hash
inline int RegisterIndexOf(uint64_t hash, int precision) {
  return (hash >> (64 - precision));
}

// Helper to count the leading zeros (less the bits used for the reg. index)
inline uint8_t ZeroCountOf(uint64_t hash, int precision) {
  // Make a mask for isolating the leading bits used for the register index.
  const uint64_t ONE = 1;
  const uint64_t mask = ~(((ONE << precision) - ONE) << (64 - precision));

  // Count zeroes, less the index bits we're masking off.
  return (CountLeadingZeroes(hash & mask) - static_cast<uint8_t>(precision));
}

}  // namespace

namespace libcount {

HLL::HLL(int precision)
    : precision_(precision), register_count_(0), registers_(NULL) {
  // The precision is vetted by the Create() function.  Assertions nonetheless.
  assert(precision >= HLL_MIN_PRECISION);
  assert(precision <= HLL_MAX_PRECISION);

  // We employ (2 ^ precision) "registers" to store max leading zeroes.
  register_count_ = (1 << precision);

  // Allocate space for the registers. We can safely economize by using bytes
  // for the counters because we know the value can't ever exceed ~60.
  registers_ = new uint8_t[register_count_];
  memset(registers_, 0, register_count_ * sizeof(registers_[0]));
}

HLL::~HLL() { delete[] registers_; }

HLL* HLL::Create(int precision, int* error) {
  if ((precision < HLL_MIN_PRECISION) || (precision > HLL_MAX_PRECISION)) {
    MaybeAssign(error, EINVAL);
    return NULL;
  }
  return new HLL(precision);
}

void HLL::Update(uint64_t hash) {
  // Which register will potentially receive the zero count of this hash?
  const int index = RegisterIndexOf(hash, precision_);
  assert(index < register_count_);

  // Count the zeroes for the hash, and add one, per the algorithm spec.
  const uint8_t count = ZeroCountOf(hash, precision_) + 1;
  assert(count <= 64);

  // Update the appropriate register if the new count is greater than current.
  if (count > registers_[index]) {
    registers_[index] = count;
  }
}

int HLL::Merge(const HLL* other) {
  assert(other != NULL);
  if (other == NULL) {
    return EINVAL;
  }

  // Ensure that the precision values of the two objects match.
  if (precision_ != other->precision_) {
    return EINVAL;
  }

  // Choose the maximum of corresponding registers from self, other and
  // store it back in self, effectively merging the state of the counters.
  for (int i = 0; i < register_count_; ++i) {
    registers_[i] = max(registers_[i], other->registers_[i]);
  }

  return 0;
}

double HLL::RawEstimate() const {
  // Let 'm' be the number of registers.
  const double m = static_cast<double>(register_count_);

  // For each register, let 'max' be the contents of the register.
  // Let 'term' be the reciprocal of 2 ^ max.
  // Finally, let 'sum' be the sum of all terms.
  double sum = 0.0;
  for (int i = 0; i < register_count_; ++i) {
    const double max = static_cast<double>(registers_[i]);
    const double term = pow(2.0, -max);
    sum += term;
  }

  // Next, calculate the harmonic mean
  const double harmonic_mean = m * (1.0 / sum);
  assert(harmonic_mean >= 0.0);

  // The harmonic mean is scaled by a constant that depends on the precision.
  const double estimate = EmpiricalAlpha(precision_) * m * harmonic_mean;
  assert(estimate >= 0.0);

  return estimate;
}

uint64_t HLL::Estimate() const {
  // TODO(tdial): The logic below was more or less copied from the research
  // paper, less the handling of the sparse register array, which is not
  // implemented at this time. It is correct, but seems a little awkward.
  // Have someone else review this.

  // First, calculate the raw estimate per original HyperLogLog.
  const double E = RawEstimate();

  // Determine the threshold under which we apply a bias correction.
  const double BiasThreshold = 5 * register_count_;

  // Calculate E', the bias corrected estimate.
  const double EP =
      (E < BiasThreshold) ? (E - EmpiricalBias(E, precision_)) : E;

  // The number of zeroed registers decides whether we use LinearCounting.
  const int V = RegistersEqualToZero();

  // H is either the LinearCounting estimate or the bias-corrected estimate.
  double H = 0.0;
  if (V != 0) {
    H = LinearCounting(register_count_, V);
  } else {
    H = EP;
  }

  // Under an empirically-determined threshold we return H, otherwise E'.
  if (H < EmpiricalThreshold(precision_)) {
    return H;
  } else {
    return EP;
  }
}

int HLL::RegistersEqualToZero() const {
  int zeroed_registers = 0;
  for (int i = 0; i < register_count_; ++i) {
    if (registers_[i] == 0) {
      ++zeroed_registers;
    }
  }
  return zeroed_registers;
}

}  // namespace libcount
