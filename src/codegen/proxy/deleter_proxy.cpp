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

DEFINE_METHOD(Deleter, Init, &peloton::codegen::Deleter::Init,
              "_ZN7peloton7codegen7Deleter4InitEPNS_"
              "11concurrency11TransactionEPNS_7storage9DataTableE");

DEFINE_METHOD(Deleter, Delete, &peloton::codegen::Deleter::Delete,
              "_ZN7peloton7codegen7Deleter6DeleteEjj");

}  // namespace codegen
}  // namespace peloton