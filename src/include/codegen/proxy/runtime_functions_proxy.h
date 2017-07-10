//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.h
//
// Identification: src/include/codegen/proxy/runtime_functions_proxy.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/proxy/proxy.h"
#include "codegen/runtime_functions.h"

namespace peloton {
namespace codegen {

PROXY(ColumnLayoutInfo) {
  PROXY_MEMBER_FIELD(0, char *, col_start_ptr);
  PROXY_MEMBER_FIELD(1, uint32_t, stride);
  PROXY_MEMBER_FIELD(2, bool, columnar);
  PROXY_TYPE("peloton::ColumnLayoutInfo", char *, uint32_t, bool);
};

PROXY(RuntimeFunctions) {
  PROXY_METHOD(CRC64Hash, &peloton::codegen::RuntimeFunctions::HashCrc64,
               "_ZN7peloton7codegen16RuntimeFunctions9HashCrc64EPKcmm");
  PROXY_METHOD(GetTileGroup, &peloton::codegen::RuntimeFunctions::GetTileGroup,
               "_ZN7peloton7codegen16RuntimeFunctions12GetTileGroupEPNS_"
               "7storage9DataTableEm");
  PROXY_METHOD(GetTileGroupLayout,
               &peloton::codegen::RuntimeFunctions::GetTileGroupLayout,
               "_ZN7peloton7codegen16RuntimeFunctions18GetTileGroupLayoutEPKNS_"
               "7storage9TileGroupEPNS1_16ColumnLayoutInfoEj");
  PROXY_METHOD(
      ThrowDivideByZeroException,
      &peloton::codegen::RuntimeFunctions::ThrowDivideByZeroException,
      "_ZN7peloton7codegen16RuntimeFunctions26ThrowDivideByZeroExceptionEv");
  PROXY_METHOD(
      ThrowOverflowException,
      &peloton::codegen::RuntimeFunctions::ThrowOverflowException,
      "_ZN7peloton7codegen16RuntimeFunctions22ThrowOverflowExceptionEv");
};

namespace proxy {
template <>
struct TypeBuilder<::peloton::codegen::RuntimeFunctions::ColumnLayoutInfo> {
  static llvm::Type *GetType(CodeGen &codegen) ALWAYS_INLINE {
    return ColumnLayoutInfoProxy::GetType(codegen);
  }
};
}  // namespace proxy

}  // namespace codegen
}  // namespace peloton
