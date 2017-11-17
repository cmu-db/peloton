//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sorter_proxy.cpp
//
// Identification: src/codegen/proxy/result_and_key_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/result_and_key_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ResultAndKey, "peloton::index::ResultAndKey", MEMBER(tuple_p),
            MEMBER(continue_key));
}
}