//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cast.h
//
// Identification: src/include/common/cast.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <limits>

#include "common/macros.h"

namespace peloton {
// Cast from signed to unsigned types.
template <typename DestinationType, typename SourceType>
DestinationType ALWAYS_ASSERT_range_cast_signed_to_unsigned(SourceType value) {
  PELOTON_ASSERT(0 == std::numeric_limits<DestinationType>::min());
  PELOTON_ASSERT(0 <= value);
  PELOTON_ASSERT(value <= std::numeric_limits<DestinationType>::max());

  return static_cast<DestinationType>(value);
}

// Cast from unsigned to signed types has a special form: don't check the lower
// bound
template <typename DestinationType, typename SourceType>
DestinationType ALWAYS_ASSERT_range_cast_unsigned_to_signed(SourceType value) {
  PELOTON_ASSERT(0 == std::numeric_limits<SourceType>::min());
  PELOTON_ASSERT(value <= std::numeric_limits<DestinationType>::max());

  return static_cast<DestinationType>(value);
}

// Cast between types of the same signs
template <typename DestinationType, typename SourceType>
DestinationType ALWAYS_ASSERT_range_cast_same(SourceType value) {
  PELOTON_ASSERT(std::numeric_limits<DestinationType>::min() <= value);
  PELOTON_ASSERT(value <= std::numeric_limits<DestinationType>::max());
  return static_cast<DestinationType>(value);
}

// Uses type traits to dispatch to the correct implementation.
template <typename DestinationType, typename SourceType,
          bool destination_signed, bool source_signed>
class ALWAYS_ASSERT_range_cast_dispatch;

template <typename DestinationType, typename SourceType>
class ALWAYS_ASSERT_range_cast_dispatch<DestinationType, SourceType, true,
                                        true> {
 public:
  DestinationType operator()(SourceType value) const {
    return ALWAYS_ASSERT_range_cast_same<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class ALWAYS_ASSERT_range_cast_dispatch<DestinationType, SourceType, false,
                                        false> {
 public:
  DestinationType operator()(SourceType value) const {
    return ALWAYS_ASSERT_range_cast_same<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class ALWAYS_ASSERT_range_cast_dispatch<DestinationType, SourceType, true,
                                        false> {
 public:
  DestinationType operator()(SourceType value) const {
    return ALWAYS_ASSERT_range_cast_unsigned_to_signed<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class ALWAYS_ASSERT_range_cast_dispatch<DestinationType, SourceType, false,
                                        true> {
 public:
  DestinationType operator()(SourceType value) const {
    return ALWAYS_ASSERT_range_cast_signed_to_unsigned<DestinationType>(value);
  }
};

// User interface
template <typename DestinationType, typename SourceType>
DestinationType ALWAYS_ASSERT_range_cast(SourceType value) {
  ALWAYS_ASSERT_range_cast_dispatch<
      DestinationType, SourceType,
      std::numeric_limits<DestinationType>::is_signed,
      std::numeric_limits<SourceType>::is_signed> dispatch;
  return dispatch(value);
}

}  // namespace peloton
