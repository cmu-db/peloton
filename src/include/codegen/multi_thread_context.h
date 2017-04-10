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

#include <atomic>

#include "codegen/codegen.h"
#include "codegen/code_context.h"
#include "codegen/function_builder.h"
#include "codegen/type.h"
#include "codegen/value.h"

namespace peloton {
namespace codegen {

class MultiThreadContext {
 public:

  MultiThreadContext(llvm::Value *thread_id, llvm::Value *num_threads, std::atomic<bool> *finished)
   : thread_id_(thread_id), num_threads_(num_threads) {
    finished_ = finished;
    if (finished_ != nullptr) {
      finished_->store(false);
    }
  }

  static llvm::Type* GetType(CodeGen& codegen) {
    static const std::string kThreadPoolTypeName = "peloton::codegen::MultiThreadContext";
    // Check if the type is already registered in the module, if so return it
    auto* thread_pool_type = codegen.LookupTypeByName(kThreadPoolTypeName);
    if (thread_pool_type != nullptr) {
      return thread_pool_type;
    }

    // Right now we don't need to define each individual field
    // since we only invoke functions on the class.
    static constexpr uint64_t thread_pool_obj_size = sizeof(MultiThreadContext);
    auto* byte_arr_type =
        llvm::ArrayType::get(codegen.Int8Type(), thread_pool_obj_size);
    thread_pool_type = llvm::StructType::create(codegen.GetContext(), {byte_arr_type},
                                            kThreadPoolTypeName);
    return thread_pool_type;
  }

  llvm::Value *GetRangeStart(CodeGen &codegen, llvm::Value *tile_group_count) {
    auto *func = GetRangeStartFunction(codegen);
    return codegen.CallFunc(func, {thread_id_, tile_group_count});
  }

  llvm::Value *GetRangeEnd(CodeGen &codegen, llvm::Value *tile_group_count) {
    auto *func = GetRangeEndFunction(codegen);
    return codegen.CallFunc(func, {thread_id_, tile_group_count});
  }

  void ThreadFinish() {
    if (finished_ != nullptr) {
      finished_->store(true);
    }
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

  llvm::Value *thread_id_;
  llvm::Value *num_threads_;

  std::atomic<bool> *finished_;
};

}
}
