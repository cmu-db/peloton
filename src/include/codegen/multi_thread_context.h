//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// multi_thread_context.h
//
// Identification: src/include/codegen/multi_thread_context.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/code_context.h"
#include "codegen/function_builder.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  llvm::Value *GetRangeStart(CodeGen &codegen, llvm::Value *thread_id, llvm::Value *tile_group_count) {
    auto *func = GetRangeStartFunction(codegen);
    return codegen.CallFunc(func, {thread_id, tile_group_count});
  }

  llvm::Value *GetRangeEnd(CodeGen &codegen, llvm::Value *thread_id, llvm::Value *tile_group_count) {
    auto *func = GetRangeEndFunction(codegen);
    return codegen.CallFunc(func, {thread_id, tile_group_count});
  }

 private:

  llvm::Function *GetRangeStartFunction(CodeGen &codegen) {
    FunctionBuilder function_builder{
        codegen.GetCodeContext(),
        "peloton1codegen1multithread1getrangestart",
        codegen.Int64Type(),
        {
            {"thread_id", codegen.Int64Type()},
            {"tile_group_count", codegen.Int64Type()}
        }};

    function_builder.ReturnAndFinish(codegen.Const64(0));
    return function_builder.GetFunction();
  }

  llvm::Function *GetRangeEndFunction(CodeGen &codegen) {
    FunctionBuilder function_builder{
        codegen.GetCodeContext(),
        "peloton1codegen1multithread1getrangeend",
        codegen.Int64Type(),
        {
            {"thread_id", codegen.Int64Type()},
            {"tile_group_count", codegen.Int64Type()}
        }};

    function_builder.ReturnAndFinish(function_builder.GetArgumentByName("tile_group_count"));
    return function_builder.GetFunction();
  }

  uint32_t thread_count;
};

}
}
