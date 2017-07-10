//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// deleter_proxy.h
//
// Identification: src/include/codegen/proxy/deleter_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/transaction_proxy.h"
#include "codegen/deleter.h"

namespace peloton {
namespace codegen {

PROXY(Deleter) {
  PROXY_MEMBER_FIELD(0, char[sizeof(Deleter)], opaque);

  PROXY_TYPE("peloton::Deleter", char[sizeof(Deleter)]);

  PROXY_METHOD(Init, &peloton::codegen::Deleter::Init,
               "_ZN7peloton7codegen7Deleter4InitEPNS_"
               "11concurrency11TransactionEPNS_7storage9DataTableE");

  PROXY_METHOD(Delete, &peloton::codegen::Deleter::Delete,
               "_ZN7peloton7codegen7Deleter6DeleteEjj");
};

namespace proxy {
template <>
struct TypeBuilder<codegen::Deleter> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return DeleterProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
