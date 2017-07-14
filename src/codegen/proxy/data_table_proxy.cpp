//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.cpp
//
// Identification: src/codegen/proxy/data_table_proxy.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(DataTable, "storage::DataTable", MEMBER(opaque));

DEFINE_METHOD(DataTable, GetTileGroupCount,
              &storage::DataTable::GetTileGroupCount,
              "_ZNK7peloton7storage9DataTable17GetTileGroupCountEv");

}  // namespace codegen
}  // namespace peloton