//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// order_by_translator.cpp
//
// Identification: src/codegen/order_by_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/order_by_translator.h"

#include "codegen/if.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/sorter_proxy.h"

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

  // Setup the storage format
  std::vector<type::Type::TypeId> tuple_desc;
  for (const auto *ai : plan_.GetOutputColumns()) {
    LOG_DEBUG("Adding %p (%s) to materialization buffer format", ai,
              TypeIdToString(ai->type).c_str());
    tuple_desc.push_back(ai->type);
  }

  // Create the sorter
  sorter_ = Sorter{codegen, tuple_desc};

  // Create the output selection vector
  output_vector_id_ = runtime_state.RegisterState(
      "obSelVec",
      codegen.VectorType(codegen.Int32Type(), Vector::kDefaultVectorSize),
      true);

  LOG_DEBUG("Finished constructing OrderByTranslator ...");
}

// Initialize the sorter instance
void OrderByTranslator::InitializeState() {
  sorter_.Init(GetCodeGen(), GetStatePtr(sorter_id_), compare_func_);
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
void OrderByTranslator::DefineFunctions() {
  LOG_DEBUG("Constructing 'compare' function for sort ...");
  auto &codegen = GetCodeGen();
  auto &storage_format = sorter_.GetStorageFormat();

  // The comparison function builder
  std::vector<std::pair<std::string, llvm::Type *>> args = {
      {"leftTuple", codegen.CharPtrType()},
      {"rightTuple", codegen.CharPtrType()}};
  FunctionBuilder cmp_function_builder{codegen.GetCodeContext(), "compare",
                                       codegen.Int32Type(), args};

  // The properly casted left and right tuples (from function argument)
  llvm::Value *left_tuple = codegen->CreateBitCast(
      cmp_function_builder.GetArgumentByName("leftTuple"),
      codegen.CharPtrType());
  llvm::Value *right_tuple = codegen->CreateBitCast(
      cmp_function_builder.GetArgumentByName("rightTuple"),
      codegen.CharPtrType());

  // TODO: FIX ME
  auto &sort_keys = plan_.GetSortKeys();
  auto &descend_flags = plan_.GetDescendFlags();
  PL_ASSERT(!sort_keys.empty());

  // First pull out all the values from materialized state
  std::vector<codegen::Value> left_vals;
  std::vector<codegen::Value> right_vals;
  for (size_t idx = 0; idx < sort_keys.size(); idx++) {
    left_vals.push_back(
        storage_format.Get(codegen, left_tuple, sort_keys[idx]));
    right_vals.push_back(
        storage_format.Get(codegen, right_tuple, sort_keys[idx]));
  }

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
  for (size_t idx = 1; idx < sort_keys.size(); idx++) {
    codegen::Value comp_result;
    if (!descend_flags[idx]) {
      // If comparison says left < right, set comp_result to -1 (0 otherwise)
      auto comparison = left_vals[idx].CompareLt(codegen, right_vals[idx]);
      comp_result = codegen::Value{
          type::Type::TypeId::INTEGER,
          codegen->CreateSelect(comparison.GetValue(), codegen.Const32(-1),
                                codegen.Const32(0))};
    } else {
      // If comparison says left > right, set comp_result to 1 (0 otherwise)
      auto comparison = right_vals[idx].CompareLt(codegen, left_vals[idx]);
      comp_result = codegen::Value{
          type::Type::TypeId::INTEGER,
          codegen->CreateSelect(comparison.GetValue(), codegen.Const32(0),
                                codegen.Const32(1))};
    }
    // If previous result is zero (meaning the values were equal), set the
    // running value to the result of the last comparison. Otherwise, carry
    // forward the comparison result of the previous attributes
    auto *prev_zero =
        codegen->CreateICmpEQ(result.GetValue(), codegen.Const32(0));
    result =
        codegen::Value{type::Type::TypeId::INTEGER,
                       codegen->CreateSelect(prev_zero, comp_result.GetValue(),
                                             result.GetValue())};
  }

  // At this point, all keys are equal (otherwise we would've jumped out)
  cmp_function_builder.ReturnAndFinish(result.GetValue());

  // Set the function pointer
  compare_func_ = cmp_function_builder.GetFunction();
}

void OrderByTranslator::Produce() const {
  LOG_DEBUG("OrderBy requesting child to produce tuples ...");

  // Let the child produce the tuples we materialize into a buffer
  GetCompilationContext().Produce(*plan_.GetChild(0));

  LOG_DEBUG("OrderBy buffered tuples into sorter, going to sort ...");

  auto &codegen = GetCodeGen();
  auto *sorter_ptr = GetStatePtr(sorter_id_);

  // The tuples have been materialized into the buffer space, NOW SORT!!!
  sorter_.Sort(codegen, sorter_ptr);

  LOG_DEBUG("OrderBy sort complete, iterating over results ...");

  // Now iterate over the sorted list
  Vector selection_vector{GetStateValue(output_vector_id_),
                          Vector::kDefaultVectorSize, codegen.Int32Type()};

  ProduceResults callback{*this, selection_vector};
  sorter_.VectorizedIterate(codegen, sorter_ptr, selection_vector.GetCapacity(),
                            callback);

  LOG_DEBUG("OrderBy completed producing tuples ...");
}

void OrderByTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Pull out the attributes we need and append the tuple into the sorter
  std::vector<codegen::Value> tuple;
  const auto &output_cols = plan_.GetOutputColumns();
  for (const auto *ai : output_cols) {
    tuple.push_back(row.GetAttribute(codegen, ai));
  }

  // Append the tuple into the sorter
  sorter_.Append(codegen, GetStatePtr(sorter_id_), tuple);
}

void OrderByTranslator::TearDownState() {
  sorter_.Destroy(GetCodeGen(), GetStatePtr(sorter_id_));
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
  RowBatch batch{start_index, end_index, selection_vector_, false};

  // Add the attribute accessors for rows in this batch
  std::vector<SorterAttributeAccess> accessors;
  auto &output_ais = translator_.GetPlan().GetOutputColumns();
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