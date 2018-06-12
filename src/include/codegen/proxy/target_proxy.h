//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// target_proxy.h
//
// Identification: src/include/codegen/proxy/target_proxy.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/proxy/proxy.h"
#include "common/internal_types.h"
#include "planner/project_info.h"

namespace peloton {
namespace codegen {

PROXY(Target) {
  DECLARE_MEMBER(0, char[sizeof(peloton::Target)], opaque);
  DECLARE_TYPE;
};

TYPE_BUILDER(Target, peloton::Target);

}  // namespace codegen
}  // namespace peloton
