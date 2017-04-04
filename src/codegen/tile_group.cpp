//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tile_group.cpp
//
// Identification: src/codegen/tile_group.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/tile_group.h"

#include "catalog/schema.h"
#include "codegen/if.h"
#include "codegen/loop.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/scan_consumer.h"
#include "codegen/tile_group_proxy.h"
#include "codegen/type.h"
#include "codegen/varlen.h"
#include "codegen/vector.h"
#include "codegen/vectorized_loop.h"
#include "codegen/type.h"
#include "type/type.h"

namespace peloton {
namespace codegen {

// TileGroup constructor
TileGroup::TileGroup(const catalog::Schema &schema) : schema_(schema) {}

/**
 * @brief This method generates code to scan over all the tuples in the provided
 * tile group. The fourth argument is allocated stack space where
 * ColumnLayoutInfo structs are, which we need to acquire the layout information
 * of columns in this tile group.
 *
 * @code
 * col_layouts := GetColumnLayouts(tile_group_ptr, column_layouts)
 * num_tuples := GetNumTuples(tile_group_ptr)
 *
 * tid := 0
 * while tid < num_tuples {
 *   ProcessTuples(tid, num_tuples, tile_group_ptr)
 *   tid := tid + 1
 * }
 * @endcode
 */
void TileGroup::GenerateTidScan(CodeGen &codegen,
                                llvm::Value *tile_group_id,
                                llvm::Value *tile_group_ptr,
                                llvm::Value *column_layouts,
                                ScanConsumer &consumer) const {
  auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, column_layouts);

  auto *num_tuples = GetNumTuples(codegen, tile_group_ptr);

  // Iterate from 0 -> num_tuples in batches of batch_size
  llvm::Value *tid = codegen.Const32(0);
  Loop tile_group_loop{
      codegen, codegen->CreateICmpULT(tid, num_tuples), {{"tid", tid}}};
  {
    tid = tile_group_loop.GetLoopVar(0);

    // Call the consumer to generate the body of the scan loop
    TileGroupAccess tile_group_access{*this, col_layouts};
    consumer.ProcessTuples(codegen, tile_group_id, tid, num_tuples, tile_group_access);

    // Move to next tuple in the tile group
    tid = codegen->CreateAdd(tid, codegen.Const32(1));
    tile_group_loop.LoopEnd(codegen->CreateICmpULT(tid, num_tuples), {tid});
  }
}

/**
 * All we do here is scan over the tuples in the tile group in vectors of the
 * given size. We let the consumer do with it as it sees fit.
 *
 * @code
 * col_layouts := GetColumnLayouts(tile_group_ptr, column_layouts)
 * num_tuples := GetNumTuples(tile_group_ptr)
 *
 * for (start := 0; start < num_tuples; start += vector_size) {
 *   end := min(start + vector_size, num_tuples)
 *   ProcessTuples(start, end, tile_group_ptr);
 * }
 * @endcode
 */
void TileGroup::GenerateVectorizedTidScan(CodeGen &codegen,
                                          llvm::Value *tile_group_id,
                                          llvm::Value *tile_group_ptr,
                                          llvm::Value *column_layouts,
                                          uint32_t vector_size,
                                          ScanConsumer &consumer) const {
  // Get the column layouts
  auto col_layouts = GetColumnLayouts(codegen, tile_group_ptr, column_layouts);

  llvm::Value *num_tuples = GetNumTuples(codegen, tile_group_ptr);
  VectorizedLoop loop{codegen, num_tuples, vector_size, {}};
  {
    VectorizedLoop::Range curr_range = loop.GetCurrentRange();

    // Pass the vector to the consumer
    TileGroupAccess tile_group_access{*this, col_layouts};
    consumer.ProcessTuples(codegen, tile_group_id,
                           curr_range.start, curr_range.end,
                           tile_group_access);

    loop.LoopEnd(codegen, {});
  }
}

//===----------------------------------------------------------------------===//
// Call TileGroup::GetNextTupleSlot(...) to determine # of tuples in tile group.
//===----------------------------------------------------------------------===//
llvm::Value *TileGroup::GetNumTuples(CodeGen &codegen,
                                     llvm::Value *tile_group) const {
  auto tg_func = TileGroupProxy::_GetNextTupleSlot::GetFunction(codegen);
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
codegen::Value TileGroup::LoadColumn(CodeGen &codegen, llvm::Value *tid,
                                     const TileGroup::ColumnLayout &layout,
                                     uint32_t vector_size) const {
  // We're calculating: col[tid] = col_start + (tid + col_stride)
  llvm::Value *col_address =
      codegen->CreateInBoundsGEP(codegen.ByteType(), layout.col_start_ptr,
                                 codegen->CreateMul(tid, layout.col_stride));

  // The value, length and is_null check
  llvm::Value *val = nullptr, *length = nullptr, *is_null = nullptr;

  // Column metadata
  const auto &column = schema_.GetColumn(layout.col_id);

  // Check if it's a string or numeric value
  if (Type::HasVariableLength(column.GetType())) {
    PL_ASSERT(!column.IsInlined());

    if (schema_.AllowNull(layout.col_id)) {
      codegen::Varlen::GetPtrAndLength(codegen, col_address, val, length,
                                       is_null);
    } else {
      codegen::Varlen::SafeGetPtrAndLength(codegen, col_address, val, length);
      is_null = codegen.ConstBool(false);
    }
    PL_ASSERT(val != nullptr && length != nullptr && is_null != nullptr);
  } else {
    // Get the LLVM type of the column
    llvm::Type *col_type = nullptr, *col_len_type = nullptr;
    Type::GetTypeForMaterialization(codegen, column.GetType(), col_type,
                                    col_len_type);
    PL_ASSERT(col_type != nullptr);
    PL_ASSERT(col_len_type == nullptr);

    if (vector_size > 1) {
      llvm::Type *vector_type = llvm::VectorType::get(col_type, vector_size);
      val = codegen->CreateAlignedLoad(
          codegen->CreateBitCast(col_address, vector_type->getPointerTo()),
          Vector::kDefaultVectorAlignment);
    } else {
      val = codegen->CreateLoad(col_type, col_address);
    }

    if (schema_.AllowNull(layout.col_id)) {
      codegen::Value tmp{column.GetType(), val};
      codegen::Value null_val = Type::GetNullValue(codegen, column.GetType());
      is_null = null_val.CompareEq(codegen, tmp).GetValue();
    } else {
      is_null = codegen.ConstBool(false);
    }
    PL_ASSERT(val != nullptr && is_null != nullptr);
  }

  // Names
  val->setName(column.GetName());
  if (length != nullptr) length->setName(column.GetName() + ".len");
  is_null->setName(column.GetName() + ".null");

  // Return the value
  return codegen::Value{column.GetType(), val, length, is_null};
}

//===----------------------------------------------------------------------===//
// TILE GROUP ROW
//===----------------------------------------------------------------------===//

TileGroup::TileGroupAccess::Row::Row(
    const TileGroup::TileGroupAccess &tile_group_access, llvm::Value *tid)
    : tile_group_access_(tile_group_access), tid_(tid) {}

codegen::Value TileGroup::TileGroupAccess::Row::LoadColumn(
    CodeGen &codegen, uint32_t col_idx, uint32_t num_vals_to_load) const {
  const auto &tile_group = tile_group_access_.GetTileGroup();
  const auto &col_layout = tile_group_access_.GetLayout(col_idx);
  return tile_group.LoadColumn(codegen, GetTID(), col_layout, num_vals_to_load);
}

//===----------------------------------------------------------------------===//
// TILE GROUP
//===----------------------------------------------------------------------===//

TileGroup::TileGroupAccess::TileGroupAccess(
    const TileGroup &tile_group,
    std::vector<TileGroup::ColumnLayout> tile_group_layout)
    : tile_group_(tile_group), layout_(tile_group_layout) {}

TileGroup::TileGroupAccess::Row TileGroup::TileGroupAccess::GetRow(
    llvm::Value *tid) const {
  return TileGroup::TileGroupAccess::Row{*this, tid};
}

}  // namespace codegen
}  // namespace peloton
