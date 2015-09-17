//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// stl_friendly_value.h
//
// Identification: src/backend/common/stl_friendly_value.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/common/value.h"

namespace peloton {

/**
 * Add a thin layer of stl-friendly behavior -- but no additional data members -- to NValue.
 * Rationale: Value shies away from heavy use of operator overloading, etc. and for some good reasons
 * But it's these kinds of things that grease the wheels for using Values in stl containers,
 * so as needed for their sake, construct or cast the Value references to StlFriendlyValue references,
 * preferably in a way that avoids copying.
 **/
struct StlFriendlyValue : public Value {
  // StlFriendlyValue instances need to be used within STL containers that support
  // methods or algorithms that require operator== support (& possibly other methods)
  // Only the minimal set of NValue constuctors (default, copy)
  // actually need be exposed through StlFriendlyNValue.

  bool operator==(const StlFriendlyValue& other) const
            {
    return Compare(other) == VALUE_COMPARE_EQUAL;
            }

  const StlFriendlyValue& operator=(const Value& other)
  {
    // Just call "super".
    (*(Value*)this) = other;
    return *this;
  }

  bool operator<(const StlFriendlyValue& other) const
  {
    return Compare(other) < VALUE_COMPARE_EQUAL;
  }

};

}  // End peloton namespace
