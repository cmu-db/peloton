//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator.cpp
//
// Identification: src/codegen/operator/order_by_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/order_by_translator.h"

#include "codegen/function_builder.h"
#include "codegen/proxy/runtime_functions_proxy.h"
#include "codegen/proxy/sorter_proxy.h"
#include "codegen/type/integer_type.h"
#include "common/logger.h"
#include "planner/order_by_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// ORDER BY TRANSLATOR
//===----------------------------------------------------------------------===//

OrderByTranslator::OrderByTranslator(const planner::OrderByPlan &plan,
                                     CompilationContext &context,
                                     Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      plan_(plan),
      child_pipeline_(this) {
  LOG_DEBUG("Constructing OrderByTranslator ...");

  // Prepare the child
  context.Prepare(*plan.GetChild(0), child_pipeline_);

  auto &codegen = GetCodeGen();

  // Register the sorter instance
  auto &runtime_state = context.GetRuntimeState();
  sorter_id_ =
      runtime_state.RegisterState("sort", SorterProxy::GetType(codegen));

  // When sorting, we need to materialize both the output columns and the sort
  // columns. These sets may overlap. To avoid duplicating storage, we track
  // the storage slot for every sort key.

  // The format of the tuple that is materialized in the sorter
  std::vector<type::Type> tuple_desc;

  // The mapping of column IDs to its position in the sorted tuple
  std::unordered_map<oid_t, uint32_t> col_id_map;

  // Every output column **must** be materialized. Add them all here.
  const auto &output_ais = plan_.GetOutputColumnAIs();
  const auto &output_col_ids = plan_.GetOutputColumnIds();
  for (uint32_t i = 0; i < output_ais.size(); i++) {
    const auto *ai = output_ais[i];
    LOG_DEBUG("Adding output column %p (%s) to format @ %u", ai,
              TypeIdToString(ai->type.type_id).c_str(), i);
    tuple_desc.push_back(ai->type);
    col_id_map.insert(std::make_pair(output_col_ids[i], i));
  }

  // Now consider the sort columns
  const auto &sort_col_ais = plan_.GetSortKeyAIs();
  const auto &sort_col_ids = plan_.GetSortKeys();
  PL_ASSERT(!sort_col_ids.empty());

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

  LOG_DEBUG("Finished constructing OrderByTranslator ...");
}

// Initialize the sorter instance
void OrderByTranslator::InitializeState() {
  sorter_.Init(GetCodeGen(), LoadStatePtr(sorter_id_), compare_func_);
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
  LOG_DEBUG("Constructing 'compare' function for sort ...");
  auto &codegen = GetCodeGen();
  auto &storage_format = sorter_.GetStorageFormat();

  // The comparison function builder
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"leftTuple", codegen.CharPtrType()},
      {"rightTuple", codegen.CharPtrType()}};
  FunctionBuilder compare{codegen.GetCodeContext(), "compare",
                          codegen.Int32Type(), args};

  // The left and right tuple (from function argument)
  llvm::Value *left_tuple = compare.GetArgumentByName("leftTuple");
  llvm::Value *right_tuple = compare.GetArgumentByName("rightTuple");

  const auto &sort_keys = plan_.GetSortKeys();
  const auto &descend_flags = plan_.GetDescendFlags();

  // First pull out all the values from materialized state
  UpdateableStorage::NullBitmap null_bitmap{codegen, storage_format,
                                            left_tuple};
  std::vector<codegen::Value> left_vals, right_vals;
  for (size_t idx = 0; idx < sort_keys.size(); idx++) {
    auto &sort_key_info = sort_key_info_[idx];
    uint32_t slot = sort_key_info.tuple_slot;

    if (!null_bitmap.IsNullable(slot)) {
      left_vals.push_back(
          storage_format.GetValueSkipNull(codegen, left_tuple, slot));
      right_vals.push_back(
          storage_format.GetValueSkipNull(codegen, right_tuple, slot));
    } else {
      left_vals.push_back(
          storage_format.GetValue(codegen, left_tuple, slot, null_bitmap));
      right_vals.push_back(
          storage_format.GetValue(codegen, right_tuple, slot, null_bitmap));
    }
  }

  // TODO: The logic here should be simplified.

  // The result of the overall comparison
  codegen::Value result;

  // The first comparison must be expensive, since this result we propagate
  // through the remaining comparison
  if (!descend_flags[0]) {
    result = left_vals[0].CompareForSort(codegen, right_vals[0]);
  } else {
    result = right_vals[0].CompareForSort(codegen, left_vals[0]);
  }

  // Do the remaining comparisons using cheaper options
  auto zero = codegen::Value{type::Integer::Instance(), codegen.Const32(0)};
  for (size_t idx = 1; idx < sort_keys.size(); idx++) {
    codegen::Value comp_result;
    if (!descend_flags[idx]) {
      comp_result = left_vals[idx].CompareForSort(codegen, right_vals[idx]);
    } else {
      comp_result = right_vals[idx].CompareForSort(codegen, left_vals[idx]);
    }
    // If previous result is zero (meaning the values were equal), set the
    // running value to the result of the last comparison. Otherwise, carry
    // forward the comparison result of the previous attributes
    auto prev_zero = result.CompareEq(codegen, zero);
    result = codegen::Value{
        type::Integer::Instance(),
        codegen->CreateSelect(prev_zero.GetValue(), comp_result.GetValue(),
                              result.GetValue())};
  }

  // At this point, all keys are equal (otherwise we would've jumped out)
  compare.ReturnAndFinish(result.GetValue());

  // Set the function pointer
  compare_func_ = compare.GetFunction();
}

void OrderByTranslator::Produce() const {
  LOG_DEBUG("OrderBy requesting child to produce tuples ...");

  // Let the child produce the tuples we materialize into a buffer
  GetCompilationContext().Produce(*plan_.GetChild(0));

  LOG_DEBUG("OrderBy buffered tuples into sorter, going to sort ...");

  auto &codegen = GetCodeGen();
  auto *sorter_ptr = LoadStatePtr(sorter_id_);

  // The tuples have been materialized into the buffer space, NOW SORT!!!
  sorter_.Sort(codegen, sorter_ptr);

  LOG_DEBUG("OrderBy sort complete, iterating over results ...");

  // Now iterate over the sorted list
  auto *raw_vec = codegen.AllocateBuffer(
      codegen.Int32Type(), Vector::kDefaultVectorSize, "orderBySelVec");
  Vector selection_vector{raw_vec, Vector::kDefaultVectorSize,
                          codegen.Int32Type()};

  ProduceResults callback{*this, selection_vector};
  sorter_.VectorizedIterate(codegen, sorter_ptr, selection_vector.GetCapacity(),
                            callback);

  LOG_DEBUG("OrderBy completed producing tuples ...");
}

void OrderByTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Pull out the attributes we need and append the tuple into the sorter
  std::vector<codegen::Value> tuple;
  const auto &output_cols = plan_.GetOutputColumnAIs();
  for (const auto *ai : output_cols) {
    tuple.push_back(row.DeriveValue(codegen, ai));
  }

  // Pop in the sort keys that are not part of the output columns
  for (const auto &sort_key_info : sort_key_info_) {
    if (!sort_key_info.is_part_of_output) {
      tuple.push_back(row.DeriveValue(codegen, sort_key_info.sort_key));
    }
  }

  // Append the tuple into the sorter
  sorter_.Append(codegen, LoadStatePtr(sorter_id_), tuple);
}

void OrderByTranslator::TearDownState() {
  sorter_.Destroy(GetCodeGen(), LoadStatePtr(sorter_id_));
}

std::string OrderByTranslator::GetName() const { return "OrderBy"; }

//===----------------------------------------------------------------------===//
// PRODUCE RESULTS
//===----------------------------------------------------------------------===//

OrderByTranslator::ProduceResults::ProduceResults(
    const OrderByTranslator &translator, Vector &selection_vector)
    : translator_(translator), selection_vector_(selection_vector) {}

void OrderByTranslator::ProduceResults::ProcessEntries(
    CodeGen &, llvm::Value *start_index, llvm::Value *end_index,
    Sorter::SorterAccess &access) const {
  // Construct the row batch we're producing
  auto &compilation_context = translator_.GetCompilationContext();
  RowBatch batch{compilation_context, start_index, end_index, selection_vector_,
                 false};

  // Add the attribute accessors for rows in this batch
  std::vector<SorterAttributeAccess> accessors;
  auto &output_ais = translator_.GetPlan().GetOutputColumnAIs();
  for (oid_t col_id = 0; col_id < output_ais.size(); col_id++) {
    accessors.emplace_back(access, col_id);
  }
  for (oid_t col_id = 0; col_id < output_ais.size(); col_id++) {
    batch.AddAttribute(output_ais[col_id], &accessors[col_id]);
  }

  // Create the context and send the batch up
  ConsumerContext context{translator_.GetCompilationContext(),
                          translator_.GetPipeline()};
  context.Consume(batch);
}

//===----------------------------------------------------------------------===//
// SORTER TUPLE ATTRIBUTE ACCESS
//===----------------------------------------------------------------------===//

OrderByTranslator::SorterAttributeAccess::SorterAttributeAccess(
    Sorter::SorterAccess &sorter_access, uint32_t col_index)
    : sorter_access_(sorter_access), col_index_(col_index) {}

Value OrderByTranslator::SorterAttributeAccess::Access(CodeGen &codegen,
                                                       RowBatch::Row &row) {
  auto &sorted_row = sorter_access_.GetRow(row.GetTID(codegen));
  return sorted_row.LoadColumn(codegen, col_index_);
}

}  // namespace codegen
}  // namespace peloton