//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_util.cpp
//
// Identification: src/storage/table_util.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/table_util.h"

#include "catalog/catalog.h"
#include "common/exception.h"
#include "common/logger.h"
#include "index/index.h"
#include "storage/data_table.h"

#include <mutex>
#include <string>

namespace peloton {
namespace storage {

std::string TableUtil::GetInfo(DataTable *table) {
  std::ostringstream os;

  os << "=====================================================\n";
  os << table->GetInfo() << std::endl;

  oid_t tile_group_count = table->GetTileGroupCount();
  // os << "Tile Group Count : " << tile_group_count << "\n";

  oid_t tuple_count = 0;
  oid_t table_id = 0;
  for (oid_t tile_group_itr = 0; tile_group_itr < tile_group_count;
       tile_group_itr++) {
    auto tile_group = table->GetTileGroup(tile_group_itr);
    table_id = tile_group->GetTableId();
    auto tile_tuple_count = tile_group->GetNextTupleSlot();

    for (oid_t tuple_id = 0; tuple_id < tile_tuple_count; tuple_id++) {
      common::Value value = tile_group->GetValue(tuple_id, 0);
    }
    os << "Tile Group Id  : " << tile_group_itr
       << " Tuple Count : " << tile_tuple_count << "\n";
    os << (*tile_group) << table_id;

    tuple_count += tile_tuple_count;
  }
  os << "=====================================================";
  return os.str();
}

}  // End storage namespace
}  // End peloton namespace
