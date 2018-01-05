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
#include "codegen/operator/projection_translator.h"
#include "codegen/proxy/sorter_proxy.h"
#include "planner/nested_loop_join_plan.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// This class implements a block-wise nested loop join. It does this by using
/// a buffer (in our case, a Sorter instance) into which tuples are buffered
/// from the left side. If this buffer is full, we call a second, generated
/// function that joins the buffer with all tuples from the right side. This
/// generated "joinBuffer" function implements the nested-loop portion. The
/// psuedocode for an INNER join would be:
///
/// function main():
///   Buffer b
///   for r in R:
///     b.insert(r)
///     if b.isFull():
///       call joinBuffer(b)
///       b.reset()
///
/// function joinBuffer(Buffer b):
///   for s in S:
///     for r in b:
///       if pred(r, s):
///         emit(r, s)
///
///
/// To facilitate this process, we **GENERATE** the "joinBuffer" function as an
/// auxiliary function. This function implements the logic for the right-side
/// query pipeline.
///
////////////////////////////////////////////////////////////////////////////////

BlockNestedLoopJoinTranslator::BlockNestedLoopJoinTranslator(
    const planner::NestedLoopJoinPlan &nlj_plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      nlj_plan_(nlj_plan),
      left_pipeline_(this) {
  PL_ASSERT(nlj_plan.GetChildrenSize() == 2 &&
            "NLJ must have exactly two children");

  // Prepare children
  context.Prepare(*nlj_plan.GetChild(0), left_pipeline_);
  context.Prepare(*nlj_plan.GetChild(1), pipeline);

  // Prepare join predicate (if one exists)
  auto *predicate = nlj_plan.GetPredicate();
  if (predicate != nullptr) {
    context.Prepare(*predicate);
  }

  // Prepare projection (if one exists)
  auto *projection = nlj_plan.GetProjInfo();
  if (projection != nullptr) {
    ProjectionTranslator::PrepareProjection(context, *projection);
  }

  // Collect all unique attributes from the left side
  for (const auto *ai : nlj_plan.GetJoinAIsLeft()) {
    unique_left_attributes_.push_back(ai);
  }
  for (const auto *ai : nlj_plan.GetLeftAttributes()) {
    if (std::find(unique_left_attributes_.begin(),
                  unique_left_attributes_.end(),
                  ai) == unique_left_attributes_.end()) {
      unique_left_attributes_.push_back(ai);
    }
  }

  // Construct the layout of tuples we store in our buffer
  std::vector<type::Type> left_input_desc;
  for (const auto *ai : unique_left_attributes_) {
    left_input_desc.push_back(ai->type);
  }

  // Allocate buffer instance in runtime state and configure its accessor
  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  buffer_id_ =
      runtime_state.RegisterState("buffer", SorterProxy::GetType(codegen));
  buffer_ = Sorter{codegen, left_input_desc};

  // Determine the number of rows to buffer before flushing it through the join
  auto max_buffer_size = settings::SettingsManager::GetDouble(
      settings::SettingId::bnlj_buffer_size);
  auto row_size = buffer_.GetStorageFormat().GetStorageSize();
  max_buf_rows_ =
      static_cast<uint32_t>(std::max(1.0, max_buffer_size / row_size));

  LOG_DEBUG(
      "Buffer size: %.2lf bytes, row size: %u.0 bytes, max buffered rows: %u",
      max_buffer_size, row_size, max_buf_rows_);
}

void BlockNestedLoopJoinTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto *null_func = codegen.Null(
      proxy::TypeBuilder<util::Sorter::ComparisonFunction>::GetType(codegen));
  buffer_.Init(codegen, LoadStatePtr(buffer_id_), null_func);
}

void BlockNestedLoopJoinTranslator::DefineAuxiliaryFunctions() {
  const auto &right_producer = *GetPlan().GetChild(1);
  auto &compilation_context = GetCompilationContext();
  join_buffer_func_ = compilation_context.DeclareAuxiliaryProducer(
      right_producer, "joinBuffer");
}

void BlockNestedLoopJoinTranslator::TearDownState() {
  buffer_.Destroy(GetCodeGen(), LoadStatePtr(buffer_id_));
}

std::string BlockNestedLoopJoinTranslator::GetName() const {
  auto max_buffer_size = settings::SettingsManager::GetDouble(
      settings::SettingId::bnlj_buffer_size);
  return StringUtil::Format("BlockNestedLoopJoin[buffer: %.2lf KB, # rows: %u]",
                            (max_buffer_size / 1024.0), max_buf_rows_);
}

void BlockNestedLoopJoinTranslator::Produce() const {
  // Let the left child produce tuples we'll batch-process in Consume()
  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  // Flush any remaining buffered tuples through the join
  auto &codegen = GetCodeGen();
  auto *num_tuples =
      buffer_.GetNumberOfStoredTuples(codegen, LoadStatePtr(buffer_id_));
  lang::If has_tuples{codegen,
                      codegen->CreateICmpUGT(num_tuples, codegen.Const32(0))};
  {
    // Flush remaining
    join_buffer_func_.Call(codegen);
  }
  has_tuples.EndIf();
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
  for (const auto &left_ai : unique_left_attributes_) {
    tuple.push_back(row.DeriveValue(codegen, left_ai));
  }

  // Append tuple to buffer
  auto *buffer_ptr = LoadStatePtr(buffer_id_);
  buffer_.Append(codegen, buffer_ptr, tuple);

  // Check if we should process the filled buffer
  auto *buf_size = buffer_.GetNumberOfStoredTuples(codegen, buffer_ptr);
  auto *flush_buffer_cond =
      codegen->CreateICmpUGE(buf_size, codegen.Const32(max_buf_rows_));
  lang::If flush_buffer{codegen, flush_buffer_cond};
  {
    // Process and reset buffer
    join_buffer_func_.Call(codegen);
    buffer_.Reset(codegen, buffer_ptr);
  }
  flush_buffer.EndIf();
}

void BlockNestedLoopJoinTranslator::ConsumeFromRight(ConsumerContext &context,
                                                     RowBatch::Row &row) const {
  // At this point, we've buffered tuples from the left input side and are
  // receiving a new input tuple from the right side. Let's find all join
  // partners with the new input tuple.
  FindMatchesForRow(context, row);
}

void BlockNestedLoopJoinTranslator::Consume(ConsumerContext &ctx,
                                            RowBatch::Row &row) const {
  if (IsFromLeftChild(ctx.GetPipeline())) {
    ConsumeFromLeft(ctx, row);
  } else {
    ConsumeFromRight(ctx, row);
  }
}

namespace {

// This is the callback called for every tuple in the buffer. There's a bit of
// ceremony, but it's just a callback function.
class BufferedTupleCallback : public Sorter::IterateCallback {
 public:
  // Constructor
  BufferedTupleCallback(
      const planner::NestedLoopJoinPlan &plan,
      const std::vector<const planner::AttributeInfo *> &left_attributes,
      ConsumerContext &ctx, RowBatch::Row &right_row);

  // The callback invoked for each tuple in the sorter/buffer
  void ProcessEntry(CodeGen &codegen,
                    const std::vector<codegen::Value> &left_row) const override;

  void ProjectAndConsume() const;

 private:
  // The plan
  const planner::NestedLoopJoinPlan &plan_;
  // The attributes produced by the left child
  const std::vector<const planner::AttributeInfo *> &left_attributes_;
  // The consumer context
  ConsumerContext &ctx_;
  // The current "outer" row
  RowBatch::Row &right_row_;
};

BufferedTupleCallback::BufferedTupleCallback(
    const planner::NestedLoopJoinPlan &plan,
    const std::vector<const planner::AttributeInfo *> &left_attributes,
    ConsumerContext &ctx, RowBatch::Row &right_row)
    : plan_(plan),
      left_attributes_(left_attributes),
      ctx_(ctx),
      right_row_(right_row) {}

// This function is called for each tuple in the BNLJ buffer.
void BufferedTupleCallback::ProcessEntry(
    CodeGen &codegen, const std::vector<codegen::Value> &left_row) const {
  PL_ASSERT(left_row.size() == left_attributes_.size());

  // Add all the attributes from left tuple (from the sorter) into the row
  // coming from the right input side. We need to do this in order to evaluate
  // the predicate.
  for (size_t i = 0; i < left_attributes_.size(); i++) {
    right_row_.RegisterAttributeValue(left_attributes_[i], left_row[i]);
  }

  auto *predicate = plan_.GetPredicate();
  if (predicate == nullptr) {
    // No predicate, just apply projection and finish
    ProjectAndConsume();
  } else {
    // Check predicate before sending to parent
    const auto &valid = right_row_.DeriveValue(codegen, *predicate);
    lang::If valid_match{codegen, valid};
    {
      // Valid tuple
      ProjectAndConsume();
    }
    valid_match.EndIf();
  }
}

void BufferedTupleCallback::ProjectAndConsume() const {
  const auto *projection_info = plan_.GetProjInfo();
  std::vector<RowBatch::ExpressionAccess> derived_attribute_access;
  if (projection_info != nullptr) {
    ProjectionTranslator::AddNonTrivialAttributes(
        right_row_.GetBatch(), *projection_info, derived_attribute_access);
  }

  // That's it, let the parent process the row
  ctx_.Consume(right_row_);
}

}  // anonymous namespace

void BlockNestedLoopJoinTranslator::FindMatchesForRow(
    ConsumerContext &ctx, RowBatch::Row &row) const {
  BufferedTupleCallback callback{GetPlan(), unique_left_attributes_, ctx, row};
  buffer_.Iterate(GetCodeGen(), LoadStatePtr(buffer_id_), callback);
}

}  // namespace codegen
}  // namespace peloton
