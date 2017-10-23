//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer_set_proxy.h
//
// Identification: include/codegen/proxy/bloom_filter_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"
#include "codegen/util/timer_set.h"

namespace peloton {
namespace codegen {

PROXY(TimerSet) {
  // Member Variables
  DECLARE_MEMBER(0, char[sizeof(util::TimerSet)], opaque);
  DECLARE_TYPE;

  // Methods
  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
  DECLARE_METHOD(Start);
  DECLARE_METHOD(Stop);
  DECLARE_METHOD(GetDuration);
};

TYPE_BUILDER(TimerSet, util::TimerSet);

}  // namespace codegen
}  // namespace peloton