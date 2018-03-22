//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// target_proxy.cpp
//
// Identification: src/codegen/proxy/target_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/target_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Target, "peloton::Target", MEMBER(opaque));
DEFINE_TYPE(Delta, "peloton::codegen::Delta", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
