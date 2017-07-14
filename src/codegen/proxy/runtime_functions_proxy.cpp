//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// runtime_functions_proxy.cpp
//
// Identification: src/codegen/proxy/runtime_functions_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/runtime_functions_proxy.h"

#include "codegen/proxy/data_table_proxy.h"
#include "codegen/proxy/tile_group_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(ColumnLayoutInfo, "peloton::ColumnLayoutInfo",
            MEMBER(col_start_ptr), MEMBER(stride), MEMBER(columnar));

DEFINE_METHOD(RuntimeFunctions, CRC64Hash,
              &peloton::codegen::RuntimeFunctions::HashCrc64,
              "_ZN7peloton7codegen16RuntimeFunctions9HashCrc64EPKcmm");
DEFINE_METHOD(RuntimeFunctions, GetTileGroup,
              &peloton::codegen::RuntimeFunctions::GetTileGroup,
              "_ZN7peloton7codegen16RuntimeFunctions12GetTileGroupEPNS_"
              "7storage9DataTableEm");
DEFINE_METHOD(RuntimeFunctions, GetTileGroupLayout,
              &peloton::codegen::RuntimeFunctions::GetTileGroupLayout,
              "_ZN7peloton7codegen16RuntimeFunctions18GetTileGroupLayoutEPKNS_"
              "7storage9TileGroupEPNS1_16ColumnLayoutInfoEj");
DEFINE_METHOD(
    RuntimeFunctions, ThrowDivideByZeroException,
    &peloton::codegen::RuntimeFunctions::ThrowDivideByZeroException,
    "_ZN7peloton7codegen16RuntimeFunctions26ThrowDivideByZeroExceptionEv");

DEFINE_METHOD(
    RuntimeFunctions, ThrowOverflowException,
    &peloton::codegen::RuntimeFunctions::ThrowOverflowException,
    "_ZN7peloton7codegen16RuntimeFunctions22ThrowOverflowExceptionEv");

}  // namespace codegen
}  // namespace peloton