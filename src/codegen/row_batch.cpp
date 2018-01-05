//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// row_batch.cpp
//
// Identification: src/codegen/row_batch.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/row_batch.h"

#include <llvm/Support/raw_ostream.h>

#include "codegen/compilation_context.h"
#include "codegen/lang/if.h"
#include "codegen/lang/vectorized_loop.h"
#include "common/exception.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

// An anonymous namespace to construct some function adapters
namespace {

//===----------------------------------------------------------------------===//
// An adapter that adapts a std::function to a full row batch iteration callback
//===----------------------------------------------------------------------===//
struct CallbackAdapter : public RowBatch::IterateCallback {
  // The callback function
  const std::function<void(RowBatch::Row &)> &callback;
  CallbackAdapter(const std::function<void(RowBatch::Row &)> &_callback)
      : callback(_callback) {}

  // The adapter
  void ProcessRow(RowBatch::Row &row) { callback(row); }
};

//===----------------------------------------------------------------------===//
// An adapter that adapts a std::function to a vectorized iteration callback
//===----------------------------------------------------------------------===//
struct VectorizedCallbackAdapter : public RowBatch::VectorizedIterateCallback {
  typedef std::function<llvm::Value *(
      RowBatch::VectorizedIterateCallback::IterationInstance &)> Callback;

  // The callback function
  uint32_t vector_size;
  const Callback &callback;

  VectorizedCallbackAdapter(uint32_t vector_size, const Callback &callback)
      : vector_size(vector_size), callback(callback) {}

  // The vector size
  uint32_t GetVectorSize() const override { return vector_size; }

  // The callback
  llvm::Value *ProcessRows(IterationInstance &iter_instance) override {
    return callback(iter_instance);
  }
};

}  // anonymous namespace

//===----------------------------------------------------------------------===//
// EXPRESSION ACCESS
//===----------------------------------------------------------------------===//

RowBatch::ExpressionAccess::ExpressionAccess(
    const expression::AbstractExpression &expression)
    : expression_(expression) {}

Value RowBatch::ExpressionAccess::Access(CodeGen &codegen, Row &row) {
  return row.DeriveValue(codegen, expression_);
}

//===----------------------------------------------------------------------===//
// ROW
//===----------------------------------------------------------------------===//

RowBatch::Row::Row(RowBatch &batch, llvm::Value *batch_pos,
                   OutputTracker *output_tracker)
    : batch_(batch),
      tid_(nullptr),
      batch_position_(batch_pos),
      output_tracker_(output_tracker) {}

bool RowBatch::Row::HasAttribute(const planner::AttributeInfo *ai) const {
  return cache_.find(ai) != cache_.end() ||
         batch_.GetAttributes().find(ai) != batch_.GetAttributes().end();
}

codegen::Value RowBatch::Row::DeriveValue(CodeGen &codegen,
                                          const planner::AttributeInfo *ai) {
  // First check cache
  auto cache_iter = cache_.find(ai);
  if (cache_iter != cache_.end()) {
    return cache_iter->second;
  }

  // Not in cache, derive it using an accessor
  auto accessor_iter = batch_.GetAttributes().find(ai);
  if (accessor_iter != batch_.GetAttributes().end()) {
    auto *accessor = accessor_iter->second;
    auto ret = accessor->Access(codegen, *this);
    cache_.insert(std::make_pair(ai, ret));
    return ret;
  }

  // Not in cache, not an attribute in this row ... crap out
  throw Exception{"Attribute '" + ai->name + "' is not an available attribute"};
}

codegen::Value RowBatch::Row::DeriveValue(
    CodeGen &codegen, const expression::AbstractExpression &expr) {
  // First check cache
  auto cache_iter = cache_.find(&expr);
  if (cache_iter != cache_.end()) {
    return cache_iter->second;
  }

  // Not in cache, derive using expression translator
  auto *translator = batch_.context_.GetTranslator(expr);
  PL_ASSERT(translator != nullptr);
  auto ret = translator->DeriveValue(codegen, *this);
  cache_.insert(std::make_pair(&expr, ret));
  return ret;
}

void RowBatch::Row::RegisterAttributeValue(const planner::AttributeInfo *ai,
                                           const codegen::Value &val) {
  // Here the caller wants to register a temporary attribute value for the row
  // that overrides any attribute accessor available for the underlying batch
  // We place the value in the cache to ensure we don't go through the normal
  // attribute accessor.
  auto iter = batch_.GetAttributes().find(ai);
  if (iter != batch_.GetAttributes().end()) {
    LOG_DEBUG(
        "Registering temporary attribute %s (%p) that overrides one available "
        "in batch",
        ai->name.c_str(), ai);
  }
  cache_.insert(std::make_pair(ai, val));
}

void RowBatch::Row::SetValidity(CodeGen &codegen, llvm::Value *valid) {
  if (valid->getType() != codegen.BoolType()) {
    std::string error_msg;
    llvm::raw_string_ostream rso{error_msg};
    rso << "Validity of row must be a boolean value. Received type: "
        << valid->getType();
    throw Exception{error_msg};
  }

  if (output_tracker_ == nullptr) {
    throw Exception{"You didn't provide an output tracker for the row!"};
  }

  // Append this row to the output
  llvm::Value *delta = codegen->CreateZExt(valid, codegen.Int32Type());
  output_tracker_->AppendRowToOutput(codegen, *this, delta);
}

llvm::Value *RowBatch::Row::GetTID(CodeGen &codegen) {
  if (tid_ == nullptr) {
    tid_ = batch_.GetPhysicalPosition(codegen, *this);
  }
  PL_ASSERT(tid_ != nullptr);
  return tid_;
}

//===----------------------------------------------------------------------===//
// OUTPUT TRACKER
//===----------------------------------------------------------------------===//

RowBatch::OutputTracker::OutputTracker(const Vector &output,
                                       llvm::Value *target_pos)
    : output_(output), target_pos_(target_pos), final_pos_(nullptr) {}

void RowBatch::OutputTracker::AppendRowToOutput(CodeGen &codegen,
                                                RowBatch::Row &row,
                                                llvm::Value *delta) {
  output_.SetValue(codegen, target_pos_, row.GetTID(codegen));
  final_pos_ = codegen->CreateAdd(target_pos_, delta);
}

llvm::Value *RowBatch::OutputTracker::GetFinalOutputPos() const {
  return final_pos_ != nullptr ? final_pos_ : target_pos_;
}

//===----------------------------------------------------------------------===//
// ROW BATCH
//===----------------------------------------------------------------------===//

RowBatch::RowBatch(CompilationContext &ctx, llvm::Value *tid_start,
                   llvm::Value *tid_end, Vector &selection_vector,
                   bool filtered)
    : RowBatch(ctx, nullptr, tid_start, tid_end, selection_vector, filtered) {}

RowBatch::RowBatch(CompilationContext &ctx, llvm::Value *tile_group_id,
                   llvm::Value *tid_start, llvm::Value *tid_end,
                   Vector &selection_vector, bool filtered)
    : context_(ctx),
      tile_group_id_(tile_group_id),
      tid_start_(tid_start),
      tid_end_(tid_end),
      num_rows_(nullptr),
      selection_vector_(selection_vector),
      filtered_(filtered) {}

void RowBatch::AddAttribute(const planner::AttributeInfo *ai,
                            RowBatch::AttributeAccess *access) {
  auto iter = attributes_.find(ai);
  if (iter != attributes_.end()) {
    LOG_DEBUG("Overwriting attribute %p with %p", iter->first, ai);
  }
  attributes_.insert(std::make_pair(ai, access));
}

// Get the row in this batch at the given position. Note: the output tracker
// is allowed to be null (hence the use of a pointer) for read-only rows.
RowBatch::Row RowBatch::GetRowAt(llvm::Value *batch_position,
                                 OutputTracker *output_tracker) {
  return RowBatch::Row{*this, batch_position, output_tracker};
}

// Iterate over all valid rows in this batch
void RowBatch::Iterate(CodeGen &codegen, RowBatch::IterateCallback &cb) {
  // The starting position in the batch
  llvm::Value *start = codegen.Const32(0);

  // The ending position in the batch
  llvm::Value *end = GetNumValidRows(codegen);

  // Generating the loop
  std::vector<lang::Loop::LoopVariable> loop_vars = {
      {"readIdx", start}, {"writeIdx", codegen.Const32(0)}};
  llvm::Value *loop_cond = codegen->CreateICmpULT(start, end);
  lang::Loop batch_loop{codegen, loop_cond, loop_vars};
  {
    // Pull out loop vars for convenience
    auto *batch_pos = batch_loop.GetLoopVar(0);
    auto *write_pos = batch_loop.GetLoopVar(1);

    // Create an output tracker to track the final position of the row
    OutputTracker tracker{GetSelectionVector(), write_pos};

    // Get the current row
    RowBatch::Row row = GetRowAt(batch_pos, &tracker);

    // Invoke callback
    cb.ProcessRow(row);

    // The next read position is one next
    auto *next_read_pos = codegen->CreateAdd(batch_pos, codegen.Const32(1));

    // The write position from the output track
    auto *next_write_pos = tracker.GetFinalOutputPos();

    // Close up loop
    llvm::Value *loop_cond = codegen->CreateICmpULT(next_read_pos, end);
    batch_loop.LoopEnd(loop_cond, {next_read_pos, next_write_pos});
  }

  // After the batch loop, we need to reset the size of the selection vector
  std::vector<llvm::Value *> final_vals;
  batch_loop.CollectFinalLoopVariables(final_vals);

  // Mark the last position in the batch
  UpdateWritePosition(final_vals[1]);
}

// Iterate over all valid rows in this batch
void RowBatch::Iterate(CodeGen &codegen,
                       const std::function<void(RowBatch::Row &)> cb) {
  // Create a simple adapter around the provided function
  CallbackAdapter adapter{cb};

  // Do iteration with adapter
  Iterate(codegen, adapter);
}

// Iterate over all valid rows in this batch in vectors of a given size
void RowBatch::VectorizedIterate(CodeGen &codegen,
                                 RowBatch::VectorizedIterateCallback &cb) {
  // The size of the vectors we use for iteration
  auto vector_size = cb.GetVectorSize();

  // The number of valid rows in the batch
  auto *num_rows = GetNumValidRows(codegen);

  // The current write/output position
  llvm::Value *write_pos = codegen.Const32(0);

  // The vectorized loop
  lang::VectorizedLoop vector_loop{
      codegen, num_rows, vector_size, {{"writePos", write_pos}}};
  {
    auto curr_range = vector_loop.GetCurrentRange();
    write_pos = vector_loop.GetLoopVar(0);

    // The current instance of the vectorized loop
    VectorizedIterateCallback::IterationInstance iter_instance;
    iter_instance.start = curr_range.start;
    iter_instance.end = curr_range.end;
    iter_instance.write_pos = write_pos;

    // Invoke the callback
    write_pos = cb.ProcessRows(iter_instance);

    // End it
    vector_loop.LoopEnd(codegen, {write_pos});
  }

  // After the loop, we need to reset the size of the selection vector
  std::vector<llvm::Value *> final_vals;
  vector_loop.CollectFinalLoopVariables(final_vals);

  // Mark the last position in the batch
  UpdateWritePosition(final_vals[1]);
}

// Iterate over all valid rows in this batch in vectors of a given size
void RowBatch::VectorizedIterate(
    CodeGen &codegen, uint32_t vector_size,
    const std::function<llvm::Value *(
        RowBatch::VectorizedIterateCallback::IterationInstance &)> &cb) {
  // Create a simple adapter around the provided function
  VectorizedCallbackAdapter adapter{vector_size, cb};

  // Invoke callback
  VectorizedIterate(codegen, adapter);
}

llvm::Value *RowBatch::GetNumValidRows(CodeGen &codegen) {
  if (IsFiltered()) {
    // The batch is filtered. Then the number of valid rows is equivalent to the
    // current size of the selection vector.
    return selection_vector_.GetNumElements();
  } else {
    // The batch isn't filtered by the selection vector. Then the number of
    // valid rows is equivalent to the total number of rows
    return GetNumTotalRows(codegen);
  }
}

llvm::Value *RowBatch::GetNumTotalRows(CodeGen &codegen) {
  if (num_rows_ == nullptr) {
    num_rows_ = codegen->CreateSub(tid_end_, tid_start_);
  }
  return num_rows_;
}

llvm::Value *RowBatch::GetTileGroupID() const { return tile_group_id_; }

void RowBatch::UpdateWritePosition(llvm::Value *sz) {
  selection_vector_.SetNumElements(sz);
  filtered_ = true;
}

llvm::Value *RowBatch::GetPhysicalPosition(CodeGen &codegen,
                                           const Row &row) const {
  llvm::Value *batch_pos = row.GetBatchPosition();
  if (IsFiltered()) {
    return selection_vector_.GetValue(codegen, batch_pos);
  } else {
    return codegen->CreateAdd(tid_start_, batch_pos);
  }
}

}  // namespace codegen
}  // namespace peloton
