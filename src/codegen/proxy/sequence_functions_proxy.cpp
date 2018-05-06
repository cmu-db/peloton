//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// sequence_functions_proxy.cpp
//
// Identification: src/codegen/proxy/sequence_functions_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/sequence_functions_proxy.h"

#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_METHOD(peloton::function, SequenceFunctions, Nextval);
DEFINE_METHOD(peloton::function, SequenceFunctions, Currval);

}  // namespace codegen
}  // namespace peloton
