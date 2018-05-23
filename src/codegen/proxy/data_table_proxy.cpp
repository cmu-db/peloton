//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// data_table_proxy.cpp
//
// Identification: src/codegen/proxy/data_table_proxy.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/proxy/data_table_proxy.h"

namespace peloton {
namespace codegen {

DEFINE_TYPE(DataTable, "storage::DataTable", opaque);

DEFINE_METHOD(peloton::storage, DataTable, GetTileGroupCount);

}  // namespace codegen
}  // namespace peloton
