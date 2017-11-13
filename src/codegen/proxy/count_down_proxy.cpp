//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// count_down_proxy.cpp
//
// Identification: src/codegen/proxy/count_down_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/count_down_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(CountDown, "codegen::CountDown", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, CountDown, Init);
DEFINE_METHOD(peloton::codegen, CountDown, Destroy);
DEFINE_METHOD(peloton::codegen, CountDown, Decrease);
DEFINE_METHOD(peloton::codegen, CountDown, Wait);

}  // namespace codegen
}  // namespace peloton
