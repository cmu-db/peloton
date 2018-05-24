//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// limit_translator.cpp
//
// Identification: src/codegen/operator/limit_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/limit_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/lang/if.h"
#include "planner/limit_plan.h"

namespace peloton {
namespace codegen {

/**
 * We implement the LIMIT operator by maintaining a counter in the query state.
 * We increment the counter for each tuple we see, and we use the counter to
 * determine whether a given tuple should be pushed to a parent operator. We
 * push a tuple to the parent if it is in the valid counter range, that is:
 *
 *   offset < count <= offset + limit
 *
 * In non-parallel mode this counter maintenance is simple. In parallel-mode,
 * we need to worry about concurrent modification of the counter. We ensure
 * correctness by generating an atomic-read-modify-write addition.
 */

LimitTranslator::LimitTranslator(const planner::LimitPlan &plan,
                                 CompilationContext &context,
                                 Pipeline &pipeline)
    : OperatorTranslator(plan, context, pipeline) {
  PELOTON_ASSERT(plan.GetChildrenSize() == 1);
  context.Prepare(*plan.GetChild(0), pipeline);

  auto &codegen = GetCodeGen();
  auto &query_state = context.GetQueryState();
  limit_count_id_ =
      query_state.RegisterState("limitCount", codegen.Int64Type());
}

void LimitTranslator::InitializeQueryState() {
  auto &codegen = GetCodeGen();

  // Initialize the counter to 0
  auto *limit_ptr = LoadStatePtr(limit_count_id_);
  codegen->CreateStore(codegen.Const64(0), limit_ptr);
}

void LimitTranslator::Produce() const {
  GetCompilationContext().Produce(*GetPlan().GetChild(0));
}

void LimitTranslator::Consume(ConsumerContext &context,
                              RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Get the pointer to the current count
  auto *limit_count_ptr = LoadStatePtr(limit_count_id_);

  llvm::Value *next_limit = nullptr;
  if (!context.GetPipeline().IsParallel()) {
    // Serial mode. Just add increment the count.
    next_limit = codegen->CreateLoad(limit_count_ptr);
    next_limit = codegen->CreateAdd(next_limit, codegen.Const64(1));
    codegen->CreateStore(next_limit, limit_count_ptr);
  } else {
    // Parallel mode. Atomically increment the count.
    next_limit = codegen->CreateAtomicRMW(
        llvm::AtomicRMWInst::BinOp::Add, limit_count_ptr, codegen.Const64(1),
        llvm::AtomicOrdering::SequentiallyConsistent);
  }

  // Pass the tuple only if in valid range
  lang::If in_window{codegen, InValidWindow(codegen, next_limit)};
  {
    // In window, send along
    context.Consume(row);
  }
  in_window.EndIf();
}

llvm::Value *LimitTranslator::InValidWindow(CodeGen &codegen,
                                            llvm::Value *count) const {
  auto &plan = GetPlanAs<planner::LimitPlan>();

  // Compute: offset < count <= (offset + limit)
  auto *gt_offset =
      codegen->CreateICmpUGT(count, codegen.Const64(plan.GetOffset()));
  auto *lt_limit = codegen->CreateICmpULE(
      count, codegen.Const64(plan.GetLimit() + plan.GetOffset()));

  return codegen->CreateAnd(gt_offset, lt_limit);
}

}  // namespace codegen
}  // namespace peloton