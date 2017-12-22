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
#include "codegen/lang/if.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/zone_map_proxy.h"
#include "storage/data_table.h"
#include "codegen/type/integer_type.h"
#include "codegen/operator/table_scan_translator.h"

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
// We acquire a Zone Map manager instance
llvm::Value *Table::GetZoneMapManager(CodeGen &codegen) const {
  return codegen.Call(ZoneMapManagerProxy::GetInstance, {});
}

// Generate a scan over all tile groups.
//
// @code
// column_layouts := alloca<peloton::ColumnLayoutInfo>(
//     table.GetSchema().GetColumnCount())
// predicate_array := alloca<peloton::PredicateInfo>(
//     num_predicates)
//
// oid_t tile_group_idx := 0
// num_tile_groups = GetTileGroupCount(table_ptr)
//
// for (; tile_group_idx < num_tile_groups; ++tile_group_idx) {
//   if (ShouldScanTileGroup(predicate_array, tile_group_idx) == true) {
//      tile_group_ptr := GetTileGroup(table_ptr, tile_group_idx)
//      consumer.TileGroupStart(tile_group_ptr);
//      tile_group.TidScan(tile_group_ptr, column_layouts, vector_size, consumer);
//      consumer.TileGroupEnd(tile_group_ptr);
//   }
// }
//
// @endcode
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                         uint32_t batch_size, ScanCallback &consumer,
                         llvm::Value *predicate_ptr,
                         size_t num_predicates) const {
  // First get the columns from the table the consumer needs. For every column,
  // we'll need to have a ColumnInfoLayout struct
  const uint32_t num_columns =
      static_cast<uint32_t>(table_.GetSchema()->GetColumnCount());

  llvm::Value *column_layouts = codegen->CreateAlloca(
      ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));

  llvm::Value *predicate_array = codegen->CreateAlloca(
      PredicateInfoProxy::GetType(codegen), codegen.Const32(num_predicates));
  if (num_predicates != 0) {
    codegen.Call(RuntimeFunctionsProxy::FillPredicateArray,
                 {predicate_ptr, predicate_array});
  }
  // Get the number of tile groups in the given table
  llvm::Value *scanned_tiles = codegen.Const64(0);
  llvm::Value *tile_group_idx = codegen.Const64(0);
  llvm::Value *num_tile_groups = GetTileGroupCount(codegen, table_ptr);
  LOG_INFO("Begin Looping over all tile groups");
  lang::Loop loop{
      codegen,
      codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
      {{"tileGroupIdx", tile_group_idx}, {"scanned_tiles", scanned_tiles}}};
  {
    // Get the tile group with the given tile group ID
    tile_group_idx = loop.GetLoopVar(0);
    scanned_tiles = loop.GetLoopVar(1);
    llvm::Value *zone_map_manager = GetZoneMapManager(codegen);
    llvm::Value *tile_group_ptr =
        GetTileGroup(codegen, table_ptr, tile_group_idx);
    llvm::Value *tile_group_id =
        tile_group_.GetTileGroupId(codegen, tile_group_ptr);
    llvm::Value *new_scanned_tiles = nullptr;
    codegen::lang::If cond{
        codegen,
        codegen.Call(
            ZoneMapManagerProxy::ShouldScanTileGroup,
            {zone_map_manager, predicate_array, codegen.Const32(num_predicates),
             table_ptr, tile_group_idx})};
    {
      // Invoke the consumer to let her know that we're starting to iterate over
      // the tile group now
      consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);
      // Generate the scan cover over the given tile group
      tile_group_.GenerateTidScan(codegen, tile_group_ptr, column_layouts,
                                  batch_size, consumer);
      // Invoke the consumer to let her know that we're done with this tile
      // group
      consumer.TileGroupFinish(codegen, tile_group_ptr);
      new_scanned_tiles = codegen->CreateAdd(scanned_tiles, codegen.Const64(1));
    }
    cond.EndIf();
    scanned_tiles = cond.BuildPHI(new_scanned_tiles, scanned_tiles);
    // Move to next tile group in the table
    tile_group_idx = codegen->CreateAdd(tile_group_idx, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                 {tile_group_idx, scanned_tiles});
  }
}

}  // namespace codegen
}  // namespace peloton
