//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.cpp
//
// Identification: src/codegen/table.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table.h"

#include "catalog/schema.h"
#include "codegen/proxy/data_table_proxy.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

// Constructor
Table::Table(storage::DataTable &table)
    : table_(table), tile_group_(*table_.GetSchema()) {}

// We determine tile group count by calling DataTable::GetTileGroupCount(...)
llvm::Value *Table::GetTileGroupCount(CodeGen &codegen,
                                      llvm::Value *table_ptr) const {
  return codegen.Call(DataTableProxy::GetTileGroupCount, {table_ptr});
}

// We acquire a tile group instance by calling RuntimeFunctions::GetTileGroup().
llvm::Value *Table::GetTileGroup(CodeGen &codegen, llvm::Value *table_ptr,
                                 llvm::Value *tile_group_id) const {
  return codegen.Call(RuntimeFunctionsProxy::GetTileGroup,
                      {table_ptr, tile_group_id});
}

// Generate a scan over all tile groups.
//
// @code
// column_layouts := alloca<peloton::ColumnLayoutInfo>(
//     table.GetSchema().GetColumnCount())
//
// oid_t tile_group_idx := 0
// num_tile_groups = GetTileGroupCount(table_ptr)
//
// for (; tile_group_idx < num_tile_groups; ++tile_group_idx) {
//   tile_group_ptr := GetTileGroup(table_ptr, tile_group_idx)
//   consumer.TileGroupStart(tile_group_ptr);
//   tile_group.TidScan(tile_group_ptr, column_layouts, vector_size, consumer);
//   consumer.TileGroupEnd(tile_group_ptr);
// }
//
// @endcode
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                         llvm::Value *tile_group_begin,
                         llvm::Value *tile_group_end,
                         uint32_t batch_size, ScanCallback &consumer) const {
  // First get the columns from the table the consumer needs. For every column,
  // we'll need to have a ColumnInfoLayout struct
  auto num_columns =
      static_cast<uint32_t>(table_.GetSchema()->GetColumnCount());

  llvm::Value *column_layouts = codegen->CreateAlloca(
      ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));

  // Get the number of tile groups in the given table
//  llvm::Value *tile_group_idx = tile_group_begin;

  // Iterate over all tile groups in the table
  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(tile_group_begin, tile_group_end),
                  {{"tileGroupIdx", tile_group_begin}}};
  {
    // Get the tile group with the given tile group ID
    llvm::Value *tile_group_idx = loop.GetLoopVar(0);
    llvm::Value *tile_group_ptr =
        GetTileGroup(codegen, table_ptr, tile_group_idx);
    llvm::Value *tile_group_id =
        tile_group_.GetTileGroupId(codegen, tile_group_ptr);

    // Invoke the consumer to let her know that we're starting to iterate over
    // the tile group now
    consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);

    // Generate the scan cover over the given tile group
    tile_group_.GenerateTidScan(codegen, tile_group_ptr, column_layouts,
                                batch_size, consumer);

    // Invoke the consumer to let her know that we're done with this tile group
    consumer.TileGroupFinish(codegen, tile_group_ptr);

    // Move to next tile group in the table
    tile_group_idx = codegen->CreateAdd(tile_group_idx, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, tile_group_end),
                 {tile_group_idx});
  }
}

}  // namespace codegen
}  // namespace peloton
