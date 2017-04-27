//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table.cpp
//
// Identification: src/codegen/table.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/table.h"

#include "catalog/schema.h"
#include "codegen/data_table_proxy.h"
#include "codegen/loop.h"
#include "codegen/runtime_functions_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

// Constructor
Table::Table(storage::DataTable &table)
    : table_(table), tile_group_(*table_.GetSchema()) {}

// We determine tile group count by calling DataTable::GetTileGroupCount(...)
llvm::Value *Table::GetTileGroupCount(CodeGen &codegen,
                                      llvm::Value *table_ptr) const {
  auto *tile_group_func =
      DataTableProxy::_GetTileGroupCount::GetFunction(codegen);
  return codegen.CallFunc(tile_group_func, {table_ptr});
}

// We acquire a tile group instance by calling RuntimeFunctions::GetTileGroup().
llvm::Value *Table::GetTileGroup(CodeGen &codegen, llvm::Value *table_ptr,
                                 llvm::Value *tile_group_id) const {
  auto *get_tg_func =
      RuntimeFunctionsProxy::_GetTileGroup::GetFunction(codegen);
  return codegen.CallFunc(get_tg_func, {table_ptr, tile_group_id});
}

// Generate the code for the scan.
//
// This method is only responsible for generating code that iterates over all
// the tile groups in the table.  It defers to codegen::TileGroup to handle the
// actual iteration over all tuples within a given tile group.
//
// As such, we just need to generate code that loops over tile groups, and make
// sure the TileGroupStart() and TileGroupFinish() callbacks are invoked on the
// scan consumer.
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                         ScanConsumer &consumer) const {
  DoGenerateScan(codegen, table_ptr, 1, consumer);
}

// Generate a vectorized scan
void Table::GenerateVectorizedScan(CodeGen &codegen, llvm::Value *table_ptr,
                                   uint32_t vector_size,
                                   ScanConsumer &consumer) const {
  DoGenerateScan(codegen, table_ptr, vector_size, consumer);
}

/**
 * Generate a scan over all tile groups.
 *
 * @code
 * column_layouts := alloca<peloton::ColumnLayoutInfo>(
 *     table.GetSchema().GetColumnCount())
 *
 * oid_t tile_group_idx := 0
 * num_tile_groups = GetTileGroupCount(table_ptr)
 *
 * for (; tile_group_idx < num_tile_groups; ++tile_group_idx) {
 *   tile_group_ptr := GetTileGroup(table_ptr, tile_group_idx)
 *   VectorizedTidScan(tile_group_ptr, column_layouts, vector_size, consumer);
 * }
 *
 * @endcode
 */
void Table::DoGenerateScan(CodeGen &codegen, llvm::Value *table_ptr,
                           uint32_t vector_size, ScanConsumer &consumer) const {
  // First get the columns from the table the consumer needs. For every column,
  // we'll need to have a ColumnInfoLayout struct
  llvm::Value *column_layouts = codegen->CreateAlloca(
      RuntimeFunctionsProxy::_ColumnLayoutInfo::GetType(codegen),
      codegen.Const32(table_.GetSchema()->GetColumnCount()));

  // Get the number of tile groups in the given table
  llvm::Value *tile_group_idx = codegen.Const64(0);
  llvm::Value *num_tile_groups = GetTileGroupCount(codegen, table_ptr);

  // Iterate over all tile groups in the table
  Loop loop{codegen,
            codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
            {{"tileGroupIdx", tile_group_idx}}};
  {
    // Get the tile group with the given tile group ID
    tile_group_idx = loop.GetLoopVar(0);
    llvm::Value *tile_group_ptr =
        GetTileGroup(codegen, table_ptr, tile_group_idx);

    // Invoke the consumer to let her know that we're starting to iterate over
    // the tile group now
    consumer.TileGroupStart(codegen, tile_group_ptr);

    // Generate the scan cover over the given tile group
    if (vector_size > 1) {
      tile_group_.GenerateVectorizedTidScan(
          codegen, tile_group_idx, tile_group_ptr, column_layouts,
          vector_size, consumer);
    } else {
      tile_group_.GenerateTidScan(codegen, tile_group_idx, tile_group_ptr,
                                  column_layouts, consumer);
    }

    // Invoke the consumer to let her know that we're done with this tile group
    consumer.TileGroupFinish(codegen, tile_group_ptr);

    // Move to next tile group in the table
    tile_group_idx = codegen->CreateAdd(tile_group_idx, codegen.Const64(1));
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                 {tile_group_idx});
  }
}

}  // namespace codegen
}  // namespace peloton