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
#include "codegen/type.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  MultiThreadContext(const CodeGen &codegen, uint64_t num_threads): codegen_(codegen) {
      num_threads_ = codegen_.Const64(num_threads);
  }

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

    auto tile_group_count = function_builder.GetArgumentByName("tile_group_count");
    auto seg_size = codegen->CreateSDiv(tile_group_count, num_threads_);
    auto thread_id = function_builder.GetArgumentByName("thread_id");
    auto start = codegen->CreateMul(thread_id, seg_size);

    function_builder.ReturnAndFinish(start);
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

    auto tile_group_count = function_builder.GetArgumentByName("tile_group_count");
    auto seg_size = codegen->CreateSDiv(tile_group_count, num_threads_);
    auto thread_id = function_builder.GetArgumentByName("thread_id");
    auto end = codegen->CreateMul(codegen->CreateAdd(thread_id, codegen.Const64(1)), seg_size);
    auto end_value = Value{type::Type::TypeId::INTEGER, end};
    end_value = end_value.Max(codegen, Value{type::Type::TypeId::INTEGER, tile_group_count});
    end = end_value.GetValue();

    function_builder.ReturnAndFinish(end);
    return function_builder.GetFunction();
  }

  llvm::Value *num_threads_;
  const CodeGen &codegen_;
};

}
}
