//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global_group_by_translator.cpp
//
// Identification: src/codegen/operator/global_group_by_translator.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/operator/global_group_by_translator.h"

#include "codegen/compilation_context.h"
#include "codegen/lang/if.h"
#include "common/logger.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

static const std::string kMatBufferTypeName = "Buffer";

GlobalGroupByTranslator::GlobalGroupByTranslator(
    const planner::AggregatePlan &plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      plan_(plan),
      child_pipeline_(this),
      aggregation_(context.GetRuntimeState()) {
  LOG_DEBUG("Constructing GlobalGroupByTranslator ...");

  auto &codegen = context.GetCodeGen();

  // Prepare the child in the new child pipeline
  context.Prepare(*plan_.GetChild(0), child_pipeline_);

  // Prepare all the aggregating expressions
  const auto &aggregates = plan_.GetUniqueAggTerms();
  for (const auto &agg_term : aggregates) {
    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }
  }

  // Setup the aggregation handler with the terms we use for aggregation
  aggregation_.Setup(codegen, aggregates, true);

  // Create the materialization buffer where we aggregate things
  auto *aggregate_storage = aggregation_.GetAggregateStorage().GetStorageType();
  PL_ASSERT(aggregate_storage->isStructTy());

  auto *mat_buffer_type = llvm::StructType::create(
      codegen.GetContext(),
      llvm::cast<llvm::StructType>(aggregate_storage)->elements(),
      kMatBufferTypeName, true);

  // Allocate state in the function argument for our materialization buffer
  auto &runtime_state = context.GetRuntimeState();
  mat_buffer_id_ = runtime_state.RegisterState("buf", mat_buffer_type);

  LOG_DEBUG("Finished constructing GlobalGroupByTranslator ...");
}

// Initialize the hash table instance
void GlobalGroupByTranslator::InitializeState() {
  aggregation_.InitializeState(GetCodeGen());
}

void GlobalGroupByTranslator::Produce() const {
  auto &codegen = GetCodeGen();

  // Initialize aggregation for global aggregation
  auto *mat_buffer = LoadStatePtr(mat_buffer_id_);
  aggregation_.CreateInitialGlobalValues(codegen, mat_buffer);

  // Let the child produce tuples that we'll aggregate
  GetCompilationContext().Produce(*plan_.GetChild(0));

  // Deserialize the finalized aggregate attribute values from the buffer
  std::vector<codegen::Value> aggregate_vals;
  aggregation_.FinalizeValues(GetCodeGen(), mat_buffer, aggregate_vals);

  std::vector<BufferAttributeAccess> buffer_accessors;

  const auto &agg_terms = plan_.GetUniqueAggTerms();
  PL_ASSERT(agg_terms.size() == aggregate_vals.size());
  for (size_t i = 0; i < agg_terms.size(); i++) {
    buffer_accessors.emplace_back(aggregate_vals, i);
  }

  // Create a row-batch of one row, place all the attributes into the row
  auto *raw_vec =
      codegen.AllocateBuffer(codegen.Int32Type(), 1, "globalGroupBySelVector");
  Vector selection_vector{raw_vec, 1, codegen.Int32Type()};
  selection_vector.SetValue(codegen, codegen.Const32(0), codegen.Const32(0));

  RowBatch batch{GetCompilationContext(), codegen.Const32(0),
                 codegen.Const32(1), selection_vector, false};

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
  // Get the updates to advance the aggregates
  auto &aggregates = plan_.GetUniqueAggTerms();
  std::vector<codegen::Value> vals{aggregates.size()};
  for (uint32_t i = 0; i < aggregates.size(); i++) {
    const auto &agg_term = aggregates[i];
    if (agg_term.expression != nullptr) {
      vals[i] = row.DeriveValue(GetCodeGen(), *agg_term.expression);
    }
  }

  // Just advance each of the aggregates in the buffer with the provided
  // new values
  aggregation_.AdvanceValues(GetCodeGen(), LoadStatePtr(mat_buffer_id_), vals);
}

// Cleanup by destroying the aggregation hash-table
void GlobalGroupByTranslator::TearDownState() {
  aggregation_.TearDownState(GetCodeGen());
}

//===----------------------------------------------------------------------===//
// Get the stringified name of this global group-by
//===----------------------------------------------------------------------===//
std::string GlobalGroupByTranslator::GetName() const { return "GlobalGroupBy"; }

}  // namespace codegen
}  // namespace peloton