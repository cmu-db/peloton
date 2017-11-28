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
#include "codegen/proxy/task_info_proxy.h"
#include "codegen/proxy/count_down_proxy.h"
#include "codegen/function_builder.h"
#include "codegen/proxy/executor_thread_pool_proxy.h"
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
void Table::GenerateScan(CodeGen &codegen, llvm::Value *table_ptr, CompilationContext &compilation_context,
                         const codegen::RuntimeState::StateID& taskinfo_vec_id, const codegen::RuntimeState::StateID& count_down_id,
                         uint32_t batch_size, ScanCallback &consumer) const {
  // First get the columns from the table the consumer needs. For every column,
  // we'll need to have a ColumnInfoLayout struct
  const uint32_t num_columns =
      static_cast<uint32_t>(table_.GetSchema()->GetColumnCount());
  (void)compilation_context;
  (void)taskinfo_vec_id;
  (void)count_down_id;

  llvm::Value *column_layouts = codegen->CreateAlloca(
    ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));

  // Get the number of tile groups in the given table
  llvm::Value *tile_group_idx = codegen.Const64(0);
  llvm::Value *num_tile_groups = GetTileGroupCount(codegen, table_ptr);

  // Iterate over all tile groups in the table
  lang::Loop loop{codegen,
                  codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                  {{"tileGroupIdx", tile_group_idx}}};
  {
    // Get the tile group with the given tile group ID
    tile_group_idx = loop.GetLoopVar(0);
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
    loop.LoopEnd(codegen->CreateICmpULT(tile_group_idx, num_tile_groups),
                 {tile_group_idx});
  }
//  auto runtime_state_ptr = codegen.GetState();
//  auto &code_context = codegen.GetCodeContext();
////  (void)runtime_state_ptr;
////  (void)code_context;
////  (void)count_down_id;
//  auto runtime_state_ptr_type = runtime_state_ptr->getType();
//  auto &runtime_state = compilation_context.GetRuntimeState();
//  Vector taskinfo_vec{runtime_state.LoadStateValue(codegen, taskinfo_vec_id),
//                      Vector::kDefaultVectorSize, codegen::TaskInfoProxy::GetType(codegen)};
//  auto taskinfo_ptr = taskinfo_vec.GetPtrToValue(codegen, codegen.Const32(0));
//   codegen::FunctionBuilder task(code_context, "task", codegen.VoidType(), {
//       {"runtime_state", runtime_state_ptr_type},
//       {"table_ptr", table_ptr->getType()}
//       {"taskinfo_ptr", TaskInfoProxy::GetType(codegen)->getPointerTo()}
//   });
//   {
//     auto taskinfo_ptr = task.GetArgumentByName("taskinfo_ptr");
//     auto &runtime_state = compilation_context.GetRuntimeState();
//
//
//     auto tile_group_idx = codegen.Call(codegen::TaskInfoProxy::GetTileGroupId, {taskinfo_ptr});
////     codegen.CallPrintf("Tile Group Index: %d \n", {tile_group_idx});
//     // Get TaskInfo by index, and execute
//     auto table_ptr = task.GetArgumentByName("table_ptr");
//     llvm::Value *tile_group_ptr = GetTileGroup(codegen, table_ptr, tile_group_idx);
//     llvm::Value *tile_group_id = tile_group_.GetTileGroupId(codegen, tile_group_ptr);
//
//     // Invoke the consumer to let her know that we're starting to iterate over
//     // the tile group now
//     consumer.TileGroupStart(codegen, tile_group_id, tile_group_ptr);
//
//     llvm::Value *column_layouts = codegen->CreateAlloca(
//     ColumnLayoutInfoProxy::GetType(codegen), codegen.Const32(num_columns));
//
//     // Generate the scan cover over the given tile group
//     tile_group_.GenerateTidScan(codegen, tile_group_ptr, column_layouts,
//                                 batch_size, consumer);
//
//     // Invoke the consumer to let her know that we're done with this tile group
//     consumer.TileGroupFinish(codegen, tile_group_ptr);
//
//     // Scan finished, decrease CountDown
//     auto count_down_ptr = runtime_state.LoadStatePtr(codegen, count_down_id);
//     codegen.Call(codegen::CountDownProxy::Decrease, {count_down_ptr});
//
//     task.ReturnAndFinish();
//   }
//
//   auto task_func_ptr = task.GetFunction();
//   auto thread_pool_ptr = codegen.Call(ExecutorThreadPoolProxy::GetInstance, {});
//   auto task_info_type = codegen::TaskInfoProxy::GetType(codegen);
//  auto task_type = llvm::FunctionType::get(
//  codegen.VoidType(), {
//                      codegen.CharPtrType(),
//                      task_info_type->getPointerTo()
//                      },
//                      false
//                      )->getPointerTo();
//   codegen.Call(codegen::ExecutorThreadPoolProxy::SubmitTask, {
//     thread_pool_ptr,
////     runtime_state_ptr,
//     codegen->CreatePointerCast(runtime_state_ptr, codegen.CharPtrType()),
//     taskinfo_ptr,
////     task_func_ptr
//     codegen->CreatePointerCast(task_func_ptr, task_type)
//   });
//   auto count_down_ptr = compilation_context.GetRuntimeState().LoadStatePtr(codegen, count_down_id);
//   codegen.Call(codegen::CountDownProxy::Wait, {count_down_ptr});
//   codegen.Call(codegen::CountDownProxy::Destroy, {count_down_ptr});
}

}  // namespace codegen
}  // namespace peloton
