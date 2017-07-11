//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.cpp
//
// Identification: src/codegen/tile_group.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tile_group.h"

#include "catalog/schema.h"
#include "codegen/lang/if.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/scan_callback.h"
#include "codegen/proxy/tile_group_proxy.h"
#include "codegen/varlen.h"
#include "codegen/vector.h"
#include "codegen/lang/vectorized_loop.h"
#include "codegen/type/boolean_type.h"

namespace peloton {
namespace codegen {

// TileGroup constructor
TileGroup::TileGroup(const catalog::Schema &schema) : schema_(schema) {}

// This method generates code to scan over all the tuples in the provided tile
// group. The fourth argument is allocated stack space where ColumnLayoutInfo
// structs are - we use this to acquire column layout information of this
// tile group.
//
// @code
// col_layouts := GetColumnLayouts(tile_group_ptr, column_layouts)
// num_tuples := GetNumTuples(tile_group_ptr)
//
// for (start := 0; start < num_tuples; start += vector_size) {
//   end := min(start + vector_size, num_tuples)
//   ProcessTuples(start, end, tile_group_ptr);
// }
// @endcode
//
void TileGroup::GenerateTidScan(CodeGen &codegen, llvm::Value *tile_group_ptr,
                                llvm::Value *column_layouts,
                                uint32_t batch_size,
                                ScanCallback &consumer) const {
  // Get the column layouts
  auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, column_layouts);

  llvm::Value *num_tuples = GetNumTuples(codegen, tile_group_ptr);
  lang::VectorizedLoop loop{codegen, num_tuples, batch_size, {}};
  {
    lang::VectorizedLoop::Range curr_range = loop.GetCurrentRange();

    // Pass the vector to the consumer
    TileGroupAccess tile_group_access{*this, col_layouts};
    consumer.ProcessTuples(codegen, curr_range.start, curr_range.end,
                           tile_group_access);

    loop.LoopEnd(codegen, {});
  }
}

// Call TileGroup::GetNextTupleSlot(...) to determine # of tuples in tile group.
llvm::Value *TileGroup::GetNumTuples(CodeGen &codegen,
                                     llvm::Value *tile_group) const {
  auto *tg_func = TileGroupProxy::_GetNextTupleSlot::GetFunction(codegen);
  return codegen.CallFunc(tg_func, {tile_group});
}

// Call TileGroup::GetTileGroupId()
llvm::Value *TileGroup::GetTileGroupId(CodeGen &codegen,
                                       llvm::Value *tile_group) const {
  auto tg_func = TileGroupProxy::_GetTileGroupId::GetFunction(codegen);
  return codegen.CallFunc(tg_func, {tile_group});
}

//===----------------------------------------------------------------------===//
// Here, we discover the layout of every column that will be accessed. A
// column's layout includes three pieces of information:
//
// 1. The starting memory address (where the first value of the column is)
// 2. The stride length
// 3. Whether the column is in columnar layout
//===----------------------------------------------------------------------===//
std::vector<TileGroup::ColumnLayout> TileGroup::GetColumnLayouts(
    CodeGen &codegen, llvm::Value *tile_group_ptr,
    llvm::Value *column_layout_infos) const {
  // Call RuntimeFunctions::GetTileGroupLayout()
  uint32_t num_cols = schema_.GetColumnCount();
  codegen.CallFunc(
      RuntimeFunctionsProxy::_GetTileGroupLayout::GetFunction(codegen),
      {tile_group_ptr, column_layout_infos, codegen.Const32(num_cols)});

  // Collect <start, stride, is_columnar> triplets of all columns
  std::vector<TileGroup::ColumnLayout> layouts;
  auto *layout_type =
      RuntimeFunctionsProxy::_ColumnLayoutInfo::GetType(codegen);
  for (uint32_t col_id = 0; col_id < num_cols; col_id++) {
    auto *start = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 0));
    auto *stride = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 1));
    auto *columnar = codegen->CreateLoad(codegen->CreateConstInBoundsGEP2_32(
        layout_type, column_layout_infos, col_id, 2));
    layouts.push_back(ColumnLayout{col_id, start, stride, columnar});
  }
  return layouts;
}

// Load a given column for the row with the given TID
codegen::Value TileGroup::LoadColumn(
    CodeGen &codegen, llvm::Value *tid,
    const TileGroup::ColumnLayout &layout) const {
  // We're calculating: col[tid] = col_start + (tid * col_stride)
  llvm::Value *col_address =
      codegen->CreateInBoundsGEP(codegen.ByteType(), layout.col_start_ptr,
                                 codegen->CreateMul(tid, layout.col_stride));

  // The value, length and is_null check
  llvm::Value *val = nullptr, *length = nullptr, *is_null = nullptr;

  // Column metadata
  bool is_nullable = schema_.AllowNull(layout.col_id);
  const auto &column = schema_.GetColumn(layout.col_id);
  const auto &sql_type = type::SqlType::LookupType(column.GetType());

  // Check if it's a string or numeric value
  if (sql_type.IsVariableLength()) {
    if (is_nullable) {
      codegen::Varlen::GetPtrAndLength(codegen, col_address, val, length,
                                       is_null);
    } else {
      codegen::Varlen::SafeGetPtrAndLength(codegen, col_address, val, length);
    }
    PL_ASSERT(val != nullptr && length != nullptr);
  } else {
    // Get the LLVM type of the column
    llvm::Type *col_type = nullptr, *col_len_type = nullptr;
    sql_type.GetTypeForMaterialization(codegen, col_type, col_len_type);
    PL_ASSERT(col_type != nullptr && col_len_type == nullptr);

    val = codegen->CreateLoad(col_type, codegen->CreateBitCast(col_address, col_type->getPointerTo()));

    if (is_nullable) {
      // To check for NULL, we need to perform a comparison between the value we
      // just read from the table with the NULL value for the column's type. We
      // need to be careful that the runtime type of both values is not NULL to
      // bypass the type system's NULL checking logic.
      auto val_tmp = codegen::Value{sql_type, val};
      auto null_val =
          codegen::Value{sql_type, sql_type.GetNullValue(codegen).GetValue()};
      auto val_is_null = val_tmp.CompareEq(codegen, null_val);
      PL_ASSERT(!val_is_null.IsNullable());
      PL_ASSERT(val_is_null.GetType() == type::Boolean::Instance());
      is_null = val_is_null.GetValue();
    }
  }

  // Names
  val->setName(column.GetName());
  if (length != nullptr) length->setName(column.GetName() + ".len");
  if (is_null != nullptr) is_null->setName(column.GetName() + ".null");

  // Return the value
  auto type = type::Type{column.GetType(), is_nullable};
  return codegen::Value{type, val, length, is_null};
}

//===----------------------------------------------------------------------===//
// TILE GROUP ROW
//===----------------------------------------------------------------------===//

TileGroup::TileGroupAccess::Row::Row(const TileGroup &tile_group,
                                     const std::vector<ColumnLayout> &layout,
                                     llvm::Value *tid)
    : tile_group_(tile_group), layout_(layout), tid_(tid) {}

codegen::Value TileGroup::TileGroupAccess::Row::LoadColumn(
    CodeGen &codegen, uint32_t col_idx) const {
  PL_ASSERT(col_idx < layout_.size());
  return tile_group_.LoadColumn(codegen, GetTID(), layout_[col_idx]);
}

//===----------------------------------------------------------------------===//
// TILE GROUP
//===----------------------------------------------------------------------===//

TileGroup::TileGroupAccess::TileGroupAccess(
    const TileGroup &tile_group,
    const std::vector<TileGroup::ColumnLayout> &tile_group_layout)
    : tile_group_(tile_group), layout_(tile_group_layout) {}

TileGroup::TileGroupAccess::Row TileGroup::TileGroupAccess::GetRow(
    llvm::Value *tid) const {
  return TileGroup::TileGroupAccess::Row{tile_group_, layout_, tid};
}

}  // namespace codegen
}  // namespace peloton
