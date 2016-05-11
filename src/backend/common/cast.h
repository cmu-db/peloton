//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cast.h
//
// Identification: src/backend/common/cast.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cassert>
#include <limits>

namespace peloton {
// Cast from signed to unsigned types.
template <typename DestinationType, typename SourceType>
DestinationType assert_range_cast_signed_to_unsigned(SourceType value) {
  assert(0 == std::numeric_limits<DestinationType>::min());
  assert(0 <= value);
  assert(value <= std::numeric_limits<DestinationType>::max());

  return static_cast<DestinationType>(value);
}

// Cast from unsigned to signed types has a special form: don't check the lower
// bound
template <typename DestinationType, typename SourceType>
DestinationType assert_range_cast_unsigned_to_signed(SourceType value) {
  assert(0 == std::numeric_limits<SourceType>::min());
  assert(value <= std::numeric_limits<DestinationType>::max());

  return static_cast<DestinationType>(value);
}

// Cast between types of the same signs
template <typename DestinationType, typename SourceType>
DestinationType assert_range_cast_same(SourceType value) {
  assert(std::numeric_limits<DestinationType>::min() <= value);
  assert(value <= std::numeric_limits<DestinationType>::max());
  return static_cast<DestinationType>(value);
}

// Uses type traits to dispatch to the correct implementation.
template <typename DestinationType, typename SourceType,
          bool destination_signed, bool source_signed>
class assert_range_cast_dispatch;

template <typename DestinationType, typename SourceType>
class assert_range_cast_dispatch<DestinationType, SourceType, true, true> {
 public:
  DestinationType operator()(SourceType value) const {
    return assert_range_cast_same<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class assert_range_cast_dispatch<DestinationType, SourceType, false, false> {
 public:
  DestinationType operator()(SourceType value) const {
    return assert_range_cast_same<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class assert_range_cast_dispatch<DestinationType, SourceType, true, false> {
 public:
  DestinationType operator()(SourceType value) const {
    return assert_range_cast_unsigned_to_signed<DestinationType>(value);
  }
};

template <typename DestinationType, typename SourceType>
class assert_range_cast_dispatch<DestinationType, SourceType, false, true> {
 public:
  DestinationType operator()(SourceType value) const {
    return assert_range_cast_signed_to_unsigned<DestinationType>(value);
  }
};

// User interface
template <typename DestinationType, typename SourceType>
DestinationType assert_range_cast(SourceType value) {
  assert_range_cast_dispatch<DestinationType, SourceType,
                             std::numeric_limits<DestinationType>::is_signed,
                             std::numeric_limits<SourceType>::is_signed>
      dispatch;
  return dispatch(value);
}

}  // namespace peloton
