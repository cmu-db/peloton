/*
   Copyright 2015 The libcount Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. See the AUTHORS file for names of
   contributors.
*/

#ifndef INCLUDE_COUNT_HLL_LIMITS_H_
#define INCLUDE_COUNT_HLL_LIMITS_H_

#ifdef __cplusplus
namespace libcount {
#endif

enum {
  /* Minimum and maximum precision values allowed. */
  HLL_MIN_PRECISION = 4,
  HLL_MAX_PRECISION = 18
};

#ifdef __cplusplus
}  // namespace libcount
#endif

#endif  // INCLUDE_COUNT_HLL_LIMITS_H_
