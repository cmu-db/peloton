//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tuple_proxy.cpp
//
// Identification: src/codegen/proxy/tuple_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/tuple_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Tuple, "storage::Tuple", MEMBER(opaque));

}  // namespace codegen
}  // namespace peloton
