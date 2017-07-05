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

#ifndef COUNT_UTILITY_H_
#define COUNT_UTILITY_H_

#include <stdint.h>

namespace libcount {

// Return the number of leading zero bits in the unsigned value.
uint8_t CountLeadingZeroes(uint64_t value);

// Equality test for doubles. Returns true if ((a - b) < epsilon).
bool IsDoubleEqual(double a, double b, double epsilon);

// If the destination pointer is valid, copy the source value to it.
// Returns true if a value was copied. Returns false otherwise.
template <typename Type>
bool MaybeAssign(Type* dest, const Type& src) {
  if (dest) {
    *dest = src;
    return true;
  }
  return false;
}

}  // namespace libcount

#endif  // COUNT_UTILITY_H_
