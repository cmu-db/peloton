//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global_group_by_translator.cpp
//
// Identification: src/codegen/operator/global_group_by_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/global_group_by_translator.h"

#include "codegen/if.h"
#include "common/logger.h"

namespace peloton {
namespace codegen {

static const std::string kMatBufferTypeName = "Buffer";

GlobalGroupByTranslator::GlobalGroupByTranslator(
    const planner::AggregatePlan &plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      plan_(plan),
      child_pipeline_(this) {
  LOG_DEBUG("Constructing GlobalGroupByTranslator ...");

  // Prepare the child in the new child pipeline
  context.Prepare(*plan_.GetChild(0), child_pipeline_);

  // Prepare all the aggregating expressions
  auto &aggregates = plan_.GetUniqueAggTerms();
  for (const auto &agg_term : aggregates) {
    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }
  }

  // Setup the aggregation handler with the terms we use for aggregation
  aggregation_.Setup(context.GetCodeGen(), aggregates);

  // Create the materialization buffer where we aggregate things
  auto &codegen = GetCodeGen();
  auto *aggregate_format = aggregation_.GetAggregateStorageFormat();
  auto *mat_buffer_type = llvm::StructType::create(
      codegen.GetContext(), {aggregate_format, codegen.ByteType()},
      kMatBufferTypeName);

  // Allocate state in the function argument for our materialization buffer
  auto &runtime_state = context.GetRuntimeState();
  mat_buffer_id_ = runtime_state.RegisterState("buf", mat_buffer_type);
  output_vector_id_ = runtime_state.RegisterState(
      "ggbSelVec", codegen.VectorType(codegen.Int32Type(), 1), true);

  LOG_DEBUG("Finished constructing GlobalGroupByTranslator ...");
}

void GlobalGroupByTranslator::Produce() const {
  // Let the child produce tuples that we aggregate in our materialization
  // buffer (in Consume())
  GetCompilationContext().Produce(*plan_.GetChild(0));

  // Deserialize the finalized aggregate attribute values from the buffer
  std::vector<codegen::Value> aggregate_vals;
  auto *mat_buffer = LoadStatePtr(mat_buffer_id_);
  aggregation_.FinalizeValues(GetCodeGen(), mat_buffer, aggregate_vals);

  std::vector<BufferAttributeAccess> buffer_accessors;

  const auto &agg_terms = plan_.GetUniqueAggTerms();
  PL_ASSERT(agg_terms.size() == aggregate_vals.size());
  for (size_t i = 0; i < agg_terms.size(); i++) {
    buffer_accessors.emplace_back(aggregate_vals, i);
  }

  auto &codegen = GetCodeGen();

  Vector v{LoadStateValue(output_vector_id_), 1, codegen.Int32Type()};
  RowBatch batch{GetCompilationContext(), codegen.Const32(0),
                 codegen.Const32(1), v, false};

  for (size_t i = 0; i < agg_terms.size(); i++) {
    batch.AddAttribute(&agg_terms[i].agg_ai, &buffer_accessors[i]);
  }

  // Create a new consumer context, put the aggregates into the context and send
  // it all up to the parent operator
  ConsumerContext context{GetCompilationContext(), GetPipeline()};
  context.Consume(batch);
}

void GlobalGroupByTranslator::Consume(ConsumerContext &,
                                      RowBatch::Row &row) const {
  auto &codegen = GetCodeGen();

  // Get the attributes we'll need to advance the aggregates
  std::vector<codegen::Value> vals;
  for (const auto &agg_term : plan_.GetUniqueAggTerms()) {
    if (agg_term.expression != nullptr) {
      vals.push_back(row.DeriveValue(codegen, *agg_term.expression));
    }
  }

  auto *mat_buffer = LoadStatePtr(mat_buffer_id_);
  auto *mat_buffer_type = codegen.LookupTypeByName(kMatBufferTypeName);

  // The buffer itself
  auto *buf =
      codegen->CreateConstInBoundsGEP2_32(mat_buffer_type, mat_buffer, 0, 0);
  // Whether the buffer has been initialized with values
  auto *initialized =
      codegen->CreateConstInBoundsGEP2_32(mat_buffer_type, mat_buffer, 0, 1);

  // Check if the buffer has been initialized. If not, create the initial values
  // and otherwise, advance the aggregates
  If uninitialized{codegen,
                   codegen->CreateICmpEQ(codegen.Const8(0),
                                         codegen->CreateLoad(initialized))};
  {
    // Create the initial values in the buffer with the ones provided
    aggregation_.CreateInitialValues(codegen, buf, vals);
    // Mark the initialized bit
    codegen->CreateStore(codegen.Const8(1), initialized);
  }
  uninitialized.ElseBlock();
  {
    // Just advance each of the aggregates in the buffer with the provided
    // new values
    aggregation_.AdvanceValues(GetCodeGen(), mat_buffer, vals);
  }
  uninitialized.EndIf();
}

//===----------------------------------------------------------------------===//
// Get the stringified name of this global group-by
//===----------------------------------------------------------------------===//
std::string GlobalGroupByTranslator::GetName() const { return "GlobalGroupBy"; }

}  // namespace codegen
}  // namespace peloton