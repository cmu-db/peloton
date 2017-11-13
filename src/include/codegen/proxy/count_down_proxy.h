//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down_proxy.h
//
// Identification: src/include/codegen/proxy/count_down_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/multi_thread/count_down.h"
#include "codegen/proxy/task_info_proxy.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/type_builder.h"

namespace peloton {
namespace codegen {

PROXY(CountDown) {
  DECLARE_MEMBER(0, char[sizeof(CountDown)], opaque);
  DECLARE_TYPE;

  DECLARE_METHOD(Init);
  DECLARE_METHOD(Destroy);
  DECLARE_METHOD(Decrease);
  DECLARE_METHOD(Wait);
};

TYPE_BUILDER(CountDown, codegen::CountDown);

}  // namespace codegen
}  // namespace peloton
