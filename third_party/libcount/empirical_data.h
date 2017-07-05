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

#ifndef COUNT_EMPIRICAL_DATA_H_
#define COUNT_EMPIRICAL_DATA_H_

namespace libcount {

// Return the empirical alpha value used for scaling harmonic means.
double EmpiricalAlpha(int precision);

// Return the cardinality threshold for the given precision value.
// Valid values for precision are [4..18] inclusive.
double EmpiricalThreshold(int precision);

// Return the empirical bias value for the raw estimate and precision.
double EmpiricalBias(double raw_estimate, int precision);

// Scan the array to determine the number of valid entries; The assumption
// is made that a value of zero marks the last entry. This is used internally
// by the implementation of the interpolation routine and is exposed so that
// it can be tested properly.
int ValidTableEntries(const double *array, int max_size);

}  // namespace libcount

#endif  // COUNT_EMPIRICAL_DATA_H_
