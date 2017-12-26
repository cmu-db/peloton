//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// block_nested_loop_join_translator.cpp
//
// Identification:
// src/codegen/operator/block_nested_loop_join_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/block_nested_loop_join_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/function_builder.h"
#include "codegen/proxy/sorter_proxy.h"
#include "planner/nested_loop_join_plan.h"

namespace peloton {
namespace codegen {

BlockNestedLoopJoinTranslator::BlockNestedLoopJoinTranslator(
    const planner::NestedLoopJoinPlan &nlj_plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      nlj_plan_(nlj_plan),
      left_pipeline_(this),
      max_buf_rows_(256) {
  // Prepare children
  PL_ASSERT(nlj_plan.GetChildrenSize() == 2 &&
            "NLJ must have only two children");
  context.Prepare(*nlj_plan.GetChild(0), left_pipeline_);
  context.Prepare(*nlj_plan.GetChild(1), pipeline);

  auto *predicate = nlj_plan.GetPredicate();
  PL_ASSERT(predicate != nullptr && "NLJ predicate cannot be NULL");
  context.Prepare(*predicate);

  auto &runtime_state = context.GetRuntimeState();
  sorter_id_ =
      runtime_state.RegisterState("sorter", SorterProxy::GetType(GetCodeGen()));

  std::vector<type::Type> left_input_desc;
  for (const auto *ai : nlj_plan.GetJoinAIsLeft()) {
    left_input_desc.push_back(ai->type);
  }
  for (const auto *ai : nlj_plan.GetLeftAttributes()) {
    left_input_desc.push_back(ai->type);
  }
  sorter_ = Sorter{GetCodeGen(), left_input_desc};
}

void BlockNestedLoopJoinTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto *null_func = codegen.Null(
      proxy::TypeBuilder<util::Sorter::ComparisonFunction>::GetType(codegen));
  sorter_.Init(codegen, LoadStatePtr(sorter_id_), null_func);
}

void BlockNestedLoopJoinTranslator::DefineAuxiliaryFunctions() {
  auto &codegen = GetCodeGen();

  auto &runtime_state = GetCompilationContext().GetRuntimeState();
  std::vector<std::pair<std::string, llvm::Type *>> args = {
      {"runtimeState", runtime_state.FinalizeType(codegen)->getPointerTo()}};
  FunctionBuilder joinBuffer{codegen.GetCodeContext(), "joinBuffer",
                             codegen.VoidType(), args};
  {
    // All this function does it call produce on the right child
    GetCompilationContext().Produce(*nlj_plan_.GetChild(1));
  }
  joinBuffer.ReturnAndFinish();
  join_buffer_func_ = joinBuffer.GetFunction();
}

void BlockNestedLoopJoinTranslator::TearDownState() {
  sorter_.Destroy(GetCodeGen(), LoadStatePtr(sorter_id_));
}

std::string BlockNestedLoopJoinTranslator::GetName() const {
  return "BlockNestedLoopJoin";
}

void BlockNestedLoopJoinTranslator::Produce() const {
  GetCompilationContext().Produce(*nlj_plan_.GetChild(0));

  // Flush any remaining tuples in buffer
  auto &codegen = GetCodeGen();
  auto *num_buffered_tuples =
      sorter_.GetNumberOfStoredTuples(codegen, LoadStatePtr(sorter_id_));
  lang::If has_tuples{
      codegen, codegen->CreateICmpUGT(num_buffered_tuples, codegen.Const32(0))};
  {
    // Flush remaining
    codegen.CallFunc(join_buffer_func_, {codegen.GetState()});
  }
  has_tuples.EndIf();
}

void BlockNestedLoopJoinTranslator::Consume(ConsumerContext &ctx,
                                            RowBatch::Row &row) const {
  if (IsFromLeftChild(ctx.GetPipeline())) {
    ConsumeFromLeft(ctx, row);
  } else {
    ConsumeFromRight(ctx, row);
  }
}

bool BlockNestedLoopJoinTranslator::IsFromLeftChild(
    const Pipeline &pipeline) const {
  return pipeline.GetChild() == left_pipeline_.GetChild();
}

void BlockNestedLoopJoinTranslator::ConsumeFromLeft(
    UNUSED_ATTRIBUTE ConsumerContext &context, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Construct tuple
  std::vector<Value> tuple;
  for (const auto &left_key_col : nlj_plan_.GetJoinAIsLeft()) {
    tuple.push_back(row.DeriveValue(codegen, left_key_col));
  }
  for (const auto &left_non_key_col : nlj_plan_.GetLeftAttributes()) {
    tuple.push_back(row.DeriveValue(codegen, left_non_key_col));
  }
  // Append tuple to buffer
  auto *sorter_ptr = LoadStatePtr(sorter_id_);
  sorter_.Append(codegen, sorter_ptr, tuple);

  // Check if we should process the filled buffer
  auto *buf_size = sorter_.GetNumberOfStoredTuples(codegen, sorter_ptr);
  auto *flush_buffer_cond =
      codegen->CreateICmpUGE(buf_size, codegen.Const32(max_buf_rows_));
  lang::If flush_buffer{codegen, flush_buffer_cond};
  {
    // Flush
    codegen.CallFunc(join_buffer_func_, {codegen.GetState()});
  }
  flush_buffer.EndIf();
}

namespace {

class BufferedTupleCallback : public Sorter::IterateCallback {
 public:
  // Constructor
  BufferedTupleCallback(const planner::NestedLoopJoinPlan &plan,
                        ConsumerContext &ctx, RowBatch::Row &row);

  // The callback invoked for each tuple in the sorter/buffer
  void ProcessEntry(
      CodeGen &codegen,
      const std::vector<codegen::Value> &left_tuple) const override;

 private:
  // The plan
  const planner::NestedLoopJoinPlan &plan_;
  // The consumer context
  ConsumerContext &ctx_;
  // The current "outer" row
  RowBatch::Row &row_;
};

inline BufferedTupleCallback::BufferedTupleCallback(
    const planner::NestedLoopJoinPlan &plan, ConsumerContext &ctx,
    RowBatch::Row &row)
    : plan_(plan), ctx_(ctx), row_(row) {}

inline void BufferedTupleCallback::ProcessEntry(
    CodeGen &codegen, const std::vector<codegen::Value> &left_tuple) const {
  const auto &left_join_key_cols = plan_.GetJoinAIsLeft();
  const auto &left_non_key_cols = plan_.GetLeftAttributes();
  PL_ASSERT(left_tuple.size() ==
            (left_join_key_cols.size() + left_non_key_cols.size()));

  // Add join keys to row first to check the predicate
  for (size_t i = 0; i < left_join_key_cols.size(); i++) {
    row_.RegisterAttributeValue(left_join_key_cols[i], left_tuple[i]);
  }

  // Check if the predicate passes now
  const auto &valid = row_.DeriveValue(codegen, *plan_.GetPredicate());
  lang::If valid_match{codegen, valid};
  {
    // We have a valid row, add the remaining non-join-key columns
    for (uint32_t i = 0; i < left_non_key_cols.size(); i++) {
      row_.RegisterAttributeValue(left_non_key_cols[i],
                                  left_tuple[i + left_join_key_cols.size()]);
    }
    ctx_.Consume(row_);
  }
  valid_match.EndIf();
}

}  // anonymous namespace

void BlockNestedLoopJoinTranslator::ConsumeFromRight(ConsumerContext &context,
                                                     RowBatch::Row &row) const {
  // At this point, the sorter instance has a buffer full of tuples. Let's
  // generate an inner loop.
  BufferedTupleCallback callback{nlj_plan_, context, row};
  sorter_.Iterate(GetCodeGen(), LoadStatePtr(sorter_id_), callback);
}

}  // namespace codegen
}  // namespace peloton
