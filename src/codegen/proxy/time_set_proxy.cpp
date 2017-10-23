//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// timer_set_proxy.cpp
//
// Identification: src/codegen/proxy/timer_set_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/timer_set_proxy.h"

namespace peloton {
namespace codegen {
    
DEFINE_TYPE(TimerSet, "peloton::util::TimerSet", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen::util, TimerSet, Init);
DEFINE_METHOD(peloton::codegen::util, TimerSet, Destroy);
DEFINE_METHOD(peloton::codegen::util, TimerSet, Start);
DEFINE_METHOD(peloton::codegen::util, TimerSet, Stop);
DEFINE_METHOD(peloton::codegen::util, TimerSet, GetDuration);
  
}  // namespace codegen
}  // namespace peloton