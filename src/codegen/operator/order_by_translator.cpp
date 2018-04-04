//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator.cpp
//
// Identification: src/codegen/operator/order_by_translator.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/order_by_translator.h"

#include "codegen/function_builder.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/sorter_proxy.h"
#include "codegen/type/integer_type.h"
#include "codegen/vector.h"
#include "common/logger.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// Sorter Attribute Access
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This class is used as a deferred accessor for an attribute from the sorter..
 */
class OrderByTranslator::SorterAttributeAccess
    : public RowBatch::AttributeAccess {
 public:
  SorterAttributeAccess(Sorter::SorterAccess &sorter_access,
                        uint32_t col_index);
  // Access the configured attributes in the provided row
  Value Access(CodeGen &codegen, RowBatch::Row &row) override;

 private:
  // A random access interface to the underlying sorter
  Sorter::SorterAccess &sorter_access_;
  // The column index of the column/attribute we want to access
  uint32_t col_index_;
};

/// Constructor
OrderByTranslator::SorterAttributeAccess::SorterAttributeAccess(
    Sorter::SorterAccess &sorter_access, uint32_t col_index)
    : sorter_access_(sorter_access), col_index_(col_index) {}

/// Access
Value OrderByTranslator::SorterAttributeAccess::Access(CodeGen &codegen,
                                                       RowBatch::Row &row) {
  auto &sorted_row = sorter_access_.GetRow(row.GetTID(codegen));
  return sorted_row.LoadColumn(codegen, col_index_);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Produce Results
///
////////////////////////////////////////////////////////////////////////////////

/**
 * This is the callback used when we're iterating over the sorted results.
 */
class OrderByTranslator::ProduceResults
    : public Sorter::VectorizedIterateCallback {
 public:
  ProduceResults(ConsumerContext &ctx, const planner::OrderByPlan &plan,
                 Vector &position_list);
  // The callback function providing the current tuple in the sorter instance
  void ProcessEntries(CodeGen &codegen, llvm::Value *start_index,
                      llvm::Value *end_index,
                      Sorter::SorterAccess &access) const override;

 private:
  // The translator
  ConsumerContext &ctx_;
  // The plan node
  const planner::OrderByPlan &plan_;
  // The selection vector when producing rows
  Vector &position_list_;
};

/// Constructor
OrderByTranslator::ProduceResults::ProduceResults(
    ConsumerContext &ctx, const planner::OrderByPlan &plan,
    Vector &position_list)
    : ctx_(ctx), plan_(plan), position_list_(position_list) {}

/// Callback to process a batch of rows from the sorter
void OrderByTranslator::ProduceResults::ProcessEntries(
    CodeGen &, llvm::Value *start_index, llvm::Value *end_index,
    Sorter::SorterAccess &access) const {
  // Construct the row batch we're producing
  auto &comp_ctx = ctx_.GetCompilationContext();
  RowBatch batch(comp_ctx, start_index, end_index, position_list_, false);

  // Add the attribute accessors for rows in this batch
  std::vector<SorterAttributeAccess> accessors;
  auto &output_ais = plan_.GetOutputColumnAIs();
  for (oid_t col_id = 0; col_id < output_ais.size(); col_id++) {
    accessors.emplace_back(access, col_id);
  }
  for (oid_t col_id = 0; col_id < output_ais.size(); col_id++) {
    batch.AddAttribute(output_ais[col_id], &accessors[col_id]);
  }

  ctx_.Consume(batch);
}

////////////////////////////////////////////////////////////////////////////////
///
/// Order By Translator
///
////////////////////////////////////////////////////////////////////////////////

OrderByTranslator::OrderByTranslator(const planner::OrderByPlan &plan,
                                     CompilationContext &context,
                                     Pipeline &pipeline)
    : OperatorTranslator(plan, context, pipeline),
      child_pipeline_(this, Pipeline::Parallelism::Flexible) {
  // Prepare the child
  context.Prepare(*plan.GetChild(0), child_pipeline_);

  CodeGen &codegen = GetCodeGen();

  // Register the sorter instance
  QueryState &query_state = context.GetQueryState();
  sorter_id_ = query_state.RegisterState("sort", SorterProxy::GetType(codegen));

  // When sorting, we need to materialize both the output columns and the sort
  // columns. These sets may overlap. To avoid duplicating storage, we track
  // the storage slot for every sort key.

  // The format of the tuple that is materialized in the sorter
  std::vector<type::Type> tuple_desc;

  // The mapping of column IDs to its position in the sorted tuple
  std::unordered_map<oid_t, uint32_t> col_id_map;

  // Every output column **must** be materialized. Add them all here.
  const auto &output_ais = plan.GetOutputColumnAIs();
  const auto &output_col_ids = plan.GetOutputColumnIds();
  for (uint32_t i = 0; i < output_ais.size(); i++) {
    const auto *ai = output_ais[i];
    LOG_DEBUG("Adding output column %p (%s) to format @ %u", ai,
              TypeIdToString(ai->type.type_id).c_str(), i);
    tuple_desc.push_back(ai->type);
    col_id_map.insert(std::make_pair(output_col_ids[i], i));
  }

  // Now consider the sort columns
  const auto &sort_col_ais = plan.GetSortKeyAIs();
  const auto &sort_col_ids = plan.GetSortKeys();
  PELOTON_ASSERT(!sort_col_ids.empty());

  for (uint32_t i = 0; i < sort_col_ais.size(); i++) {
    const auto *ai = sort_col_ais[i];

    // Let's check if the current sort column references an output column. If
    // so, we only need to configure the mapping. If the sort column references
    // a non-output column, it must be materialized.

    auto iter = col_id_map.find(sort_col_ids[i]);

    SortKeyInfo sort_key_info;

    if (iter != col_id_map.end()) {
      LOG_DEBUG("Sort column %p (%s) references output column @ %u", ai,
                TypeIdToString(ai->type.type_id).c_str(), iter->second);
      sort_key_info = {ai, true, iter->second};
    } else {
      LOG_DEBUG("Adding sort column %p (%s) to tuple format @ %u", ai,
                TypeIdToString(ai->type.type_id).c_str(),
                static_cast<uint32_t>(tuple_desc.size()));
      tuple_desc.push_back(ai->type);
      sort_key_info = {ai, false, static_cast<uint32_t>(tuple_desc.size() - 1)};
    }

    sort_key_info_.push_back(sort_key_info);
  }

  // Create the sorter
  sorter_ = Sorter{codegen, tuple_desc};
}

void OrderByTranslator::InitializeQueryState() {
  auto *sorter_ptr = LoadStatePtr(sorter_id_);
  auto *exec_ctx_ptr = GetExecutorContextPtr();
  sorter_.Init(GetCodeGen(), sorter_ptr, exec_ctx_ptr, compare_func_);
}

void OrderByTranslator::TearDownQueryState() {
  sorter_.Destroy(GetCodeGen(), LoadStatePtr(sorter_id_));
}

//===----------------------------------------------------------------------===//
// Here, we define the primary comparison function that is used to sort input
// tuples.  The function should return:
//   => (-1) if left_tuple < right_tuple in the sort order
//   =>  (0) if left_tuple == right_tuple in the sort order
//   =>  (1) if the left_tuple > right_tuple in the sort order
//
// In essence, we create a function with the following simplified logic:
//
// int compare(leftTuple, rightTuple) {
//   for (key : sort_keys) {
//     leftVal = leftTuple.getVal(key)
//     rightVal = rightTuple.getVal(key)
//     if (leftVal < rightVal) {
//       return -1;
//     } else if (leftVal > rightVal) {
//       return 1;
//     }
//   }
//   return 0
// }
//
// We of course need to take care of whether the given sort key is in ascending
// or descending order and worry about types etc.
//===----------------------------------------------------------------------===//
void OrderByTranslator::DefineAuxiliaryFunctions() {
  CodeGen &codegen = GetCodeGen();
  const auto &plan = GetPlanAs<planner::OrderByPlan>();

  const auto &storage_format = sorter_.GetStorageFormat();

  const auto &sort_keys = plan.GetSortKeys();
  const auto &descend_flags = plan.GetDescendFlags();

  // The comparison function builder
  auto *ret_type = codegen.Int32Type();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"leftTuple", codegen.CharPtrType()},
      {"rightTuple", codegen.CharPtrType()}};
  FunctionBuilder compare(codegen.GetCodeContext(), "compare", ret_type, args);
  {
    llvm::Value *left_tuple = compare.GetArgumentByPosition(0);
    llvm::Value *right_tuple = compare.GetArgumentByPosition(1);

    UpdateableStorage::NullBitmap left_null_bitmap(codegen, storage_format,
                                                   left_tuple);
    UpdateableStorage::NullBitmap right_null_bitmap(codegen, storage_format,
                                                    right_tuple);

    // The result of the overall comparison
    codegen::Value result;
    codegen::Value zero(type::Integer::Instance(), codegen.Const32(0));

    for (size_t idx = 0; idx < sort_keys.size(); idx++) {
      auto &sort_key_info = sort_key_info_[idx];
      uint32_t slot = sort_key_info.tuple_slot;

      // Read values from storage
      codegen::Value left =
          storage_format.GetValue(codegen, left_tuple, slot, left_null_bitmap);
      codegen::Value right = storage_format.GetValue(codegen, right_tuple, slot,
                                                     right_null_bitmap);

      // Perform comparison
      codegen::Value cmp;
      if (!descend_flags[idx]) {
        cmp = left.CompareForSort(codegen, right);
      } else {
        cmp = right.CompareForSort(codegen, left);
      }

      if (idx == 0) {
        result = cmp;
      } else {
        // If previous result is zero (meaning the values were equal), set the
        // running value to the result of the last comparison. Otherwise, carry
        // forward the comparison result of the previous attributes
        auto prev_zero = result.CompareEq(codegen, zero);
        result = codegen::Value(
            type::Integer::Instance(),
            codegen->CreateSelect(prev_zero.GetValue(), cmp.GetValue(),
                                  result.GetValue()));
      }
    }

    // Return result
    compare.ReturnAndFinish(result.GetValue());
  }
  // Set the function pointer
  compare_func_ = compare.GetFunction();
}

void OrderByTranslator::Produce() const {
  // Let the child produce the tuples we materialize into a buffer
  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  auto producer = [this](ConsumerContext &ctx) {
    CodeGen &codegen = GetCodeGen();
    auto *sorter_ptr = LoadStatePtr(sorter_id_);

#if 0
    // The tuples have been materialized into the buffer space, NOW SORT!!!
    if (!child_pipeline_.IsParallel()) {
      sorter_.Sort(codegen, sorter_ptr);
    }
#endif

    // Now iterate over the sorted list
    auto *i32_type = codegen.Int32Type();
    auto vec_size = Vector::kDefaultVectorSize.load();
    auto *raw_vec = codegen.AllocateBuffer(i32_type, vec_size, "obPosList");
    Vector position_list(raw_vec, vec_size, i32_type);

    const auto &plan = GetPlanAs<planner::OrderByPlan>();
    ProduceResults callback(ctx, plan, position_list);
    sorter_.VectorizedIterate(codegen, sorter_ptr, vec_size, callback);
  };

  GetPipeline().RunSerial(producer);
}

void OrderByTranslator::Consume(ConsumerContext &ctx,
                                RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

  // Pull out the attributes we need and append the tuple into the sorter
  const auto &plan = GetPlanAs<planner::OrderByPlan>();
  std::vector<codegen::Value> tuple;
  const auto &output_cols = plan.GetOutputColumnAIs();
  for (const auto *ai : output_cols) {
    tuple.push_back(row.DeriveValue(codegen, ai));
  }

  // Pop in the sort keys that are not part of the output columns
  for (const auto &sort_key_info : sort_key_info_) {
    if (!sort_key_info.is_part_of_output) {
      tuple.push_back(row.DeriveValue(codegen, sort_key_info.sort_key));
    }
  }

  // Get the right sorter pointer
  llvm::Value *sorter_ptr = nullptr;
  if (ctx.GetPipeline().IsParallel()) {
    auto *pipeline_ctx = ctx.GetPipelineContext();
    sorter_ptr = pipeline_ctx->LoadStatePtr(codegen, thread_sorter_id_);
  } else {
    sorter_ptr = LoadStatePtr(sorter_id_);
  }

  // Append the tuple into the sorter
  sorter_.Append(codegen, sorter_ptr, tuple);
}

void OrderByTranslator::RegisterPipelineState(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() == child_pipeline_ &&
      pipeline_ctx.IsParallel()) {
    auto *sorter_type = SorterProxy::GetType(GetCodeGen());
    thread_sorter_id_ = pipeline_ctx.RegisterState("sorter", sorter_type);
  }
}

void OrderByTranslator::InitializePipelineState(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() == child_pipeline_ &&
      pipeline_ctx.IsParallel()) {
    CodeGen &codegen = GetCodeGen();
    auto *sorter_ptr = pipeline_ctx.LoadStatePtr(codegen, thread_sorter_id_);
    auto *exec_ctx_ptr = GetExecutorContextPtr();
    sorter_.Init(codegen, sorter_ptr, exec_ctx_ptr, compare_func_);
  }
}

void OrderByTranslator::FinishPipeline(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() != child_pipeline_) {
    return;
  }

  CodeGen &codegen = GetCodeGen();
  auto *sorter_ptr = LoadStatePtr(sorter_id_);
  if (pipeline_ctx.IsParallel()) {
    auto *thread_states_ptr = GetThreadStatesPtr();
    auto offset = pipeline_ctx.GetEntryOffset(codegen, thread_sorter_id_);
    sorter_.SortParallel(codegen, sorter_ptr, thread_states_ptr, offset);
  } else {
    sorter_.Sort(codegen, sorter_ptr);
  }
}

void OrderByTranslator::TearDownPipelineState(PipelineContext &pipeline_ctx) {
  if (pipeline_ctx.GetPipeline() == child_pipeline_ &&
      pipeline_ctx.IsParallel()) {
    CodeGen &codegen = GetCodeGen();
    auto *sorter_ptr = pipeline_ctx.LoadStatePtr(codegen, thread_sorter_id_);
    sorter_.Destroy(codegen, sorter_ptr);
  }
}

}  // namespace codegen
}  // namespace peloton