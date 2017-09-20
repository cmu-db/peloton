//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter_proxy.cpp
//
// Identification: src/codegen/proxy/deleter_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/deleter_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/transaction_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(Deleter, "codegen::Deleter", MEMBER(opaque));

DEFINE_METHOD(peloton::codegen, Deleter, Init);
DEFINE_METHOD(peloton::codegen, Deleter, Delete);

}  // namespace codegen
}  // namespace peloton
