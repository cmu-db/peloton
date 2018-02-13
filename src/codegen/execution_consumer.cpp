//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// execution_consumer.cpp
//
// Identification: src/codegen/execution_consumer.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/execution_consumer.h"

#include "codegen/compilation_context.h"
#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

ExecutionConsumer::ExecutionConsumer() : executor_ctx_type_(nullptr) {}

void ExecutionConsumer::Prepare(CompilationContext &compilation_ctx) {
  auto &codegen = compilation_ctx.GetCodeGen();
  auto &query_state = compilation_ctx.GetQueryState();
  executor_ctx_type_ = ExecutorContextProxy::GetType(codegen);
  executor_ctx_id_ = query_state.RegisterState(
      "executorContext", executor_ctx_type_->getPointerTo());
}

void ExecutionConsumer::InitializePipelineState(PipelineContext &pipeline_ctx) {
  // TODO(pmenon): This is nasty. We should move the parameters to this class...
  auto &compilation_ctx = pipeline_ctx.GetPipeline().GetCompilationContext();
  auto &paramter_cache = compilation_ctx.GetParameterCache();
  paramter_cache.Reset();
  paramter_cache.Populate(compilation_ctx.GetCodeGen(),
                          GetQueryParametersPtr(compilation_ctx));
}

void ExecutionConsumer::ConsumeResult(ConsumerContext &context,
                                      RowBatch &batch) const {
  // Just iterate over every row in the batch
  batch.Iterate(context.GetCodeGen(), [this, &context](RowBatch::Row &row) {
    ConsumeResult(context, row);
  });
}

llvm::Value *ExecutionConsumer::GetExecutorContextPtr(
    CompilationContext &compilation_ctx) {
  auto &query_state = compilation_ctx.GetQueryState();
  return query_state.LoadStateValue(compilation_ctx.GetCodeGen(),
                                    executor_ctx_id_);
}

llvm::Value *ExecutionConsumer::GetTransactionPtr(
    CompilationContext &compilation_ctx) {
  auto &codegen = compilation_ctx.GetCodeGen();
  auto *exec_ctx_ptr = GetExecutorContextPtr(compilation_ctx);
  auto *addr = codegen->CreateConstInBoundsGEP2_32(executor_ctx_type_,
                                                   exec_ctx_ptr, 0, 1);
  return codegen->CreateLoad(addr, "transactionPtr");
}

llvm::Value *ExecutionConsumer::GetStorageManagerPtr(
    CompilationContext &compilation_ctx) {
  auto &codegen = compilation_ctx.GetCodeGen();
  auto *exec_ctx_ptr = GetExecutorContextPtr(compilation_ctx);
  auto *addr = codegen->CreateConstInBoundsGEP2_32(executor_ctx_type_,
                                                   exec_ctx_ptr, 0, 3);
  return codegen->CreateLoad(addr, "storageMgrPtr");
}

llvm::Value *ExecutionConsumer::GetQueryParametersPtr(
    CompilationContext &compilation_ctx) {
  auto &codegen = compilation_ctx.GetCodeGen();
  auto *exec_ctx_ptr = GetExecutorContextPtr(compilation_ctx);
  return codegen->CreateConstInBoundsGEP2_32(executor_ctx_type_, exec_ctx_ptr,
                                             0, 2, "queryParamsPtr");
}

llvm::Value *ExecutionConsumer::GetThreadStatesPtr(
    CompilationContext &compilation_ctx) {
  auto &codegen = compilation_ctx.GetCodeGen();
  auto *exec_ctx_ptr = GetExecutorContextPtr(compilation_ctx);
  return codegen->CreateConstInBoundsGEP2_32(executor_ctx_type_, exec_ctx_ptr,
                                             0, 5, "threadStatesPtr");
}

}  // namespace codegen
}  // namespace peloton
