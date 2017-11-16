//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.cpp
//
// Identification: src/codegen/art_index.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/art_index.h"

#include "catalog/schema.h"
//#include "codegen/proxy/data_table_proxy.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "index/art_index.h"

namespace peloton {
namespace codegen {

// Constructor
ArtIndex::ArtIndex(index::Index &index) : art_index_(dynamic_cast<index::ArtIndex &>(index)) {}

}
}

