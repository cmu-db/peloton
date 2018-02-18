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

GlobalGroupByTranslator::GlobalGroupByTranslator(
    const planner::AggregatePlan &plan, CompilationContext &context,
    Pipeline &pipeline)
    : OperatorTranslator(plan, context, pipeline),
      child_pipeline_(this),
      aggregation_(context.GetQueryState()) {
  LOG_DEBUG("Constructing GlobalGroupByTranslator ...");

  CodeGen &codegen = context.GetCodeGen();

  // Prepare the child in the new child pipeline
  context.Prepare(*plan.GetChild(0), child_pipeline_);

  // Prepare all the aggregating expressions
  const auto &aggregates = plan.GetUniqueAggTerms();
  for (const auto &agg_term : aggregates) {
    if (agg_term.expression != nullptr) {
      context.Prepare(*agg_term.expression);
    }
  }

  // Setup the aggregation handler with the terms we use for aggregation
  aggregation_.Setup(codegen, aggregates, true);

  // Create the materialization buffer where we aggregate things
  auto *aggregate_storage = aggregation_.GetAggregateStorage().GetStorageType();
  PELOTON_ASSERT(aggregate_storage->isStructTy());

  auto *mat_buffer_type = llvm::StructType::create(
      codegen.GetContext(),
      llvm::cast<llvm::StructType>(aggregate_storage)->elements(), "Buffer",
      true);

  // Allocate state in the function argument for our materialization buffer
  QueryState &query_state = context.GetQueryState();
  mat_buffer_id_ = query_state.RegisterState("buf", mat_buffer_type);

  LOG_DEBUG("Finished constructing GlobalGroupByTranslator ...");
}

// Initialize the hash table instance
void GlobalGroupByTranslator::InitializeQueryState() {
  aggregation_.InitializeQueryState(GetCodeGen());
}

void GlobalGroupByTranslator::Produce() const {
  // Initialize aggregation for global aggregation
  aggregation_.CreateInitialGlobalValues(GetCodeGen(),
                                         LoadStatePtr(mat_buffer_id_));

  // Let the child produce tuples that we'll aggregate
  GetCompilationContext().Produce(*GetPlan().GetChild(0));

  auto producer = [this](ConsumerContext &ctx) {
    CodeGen &codegen = GetCodeGen();
    auto *raw_vec =
        codegen.AllocateBuffer(codegen.Int32Type(), 1, "globalGbSelVector");
    Vector selection_vector{raw_vec, 1, codegen.Int32Type()};
    selection_vector.SetValue(codegen, codegen.Const32(0), codegen.Const32(0));

    // Create a row-batch of one row, place all the attributes into the row
    RowBatch batch{GetCompilationContext(), codegen.Const32(0),
                   codegen.Const32(1), selection_vector, false};

    // Deserialize the finalized aggregate attribute values from the buffer
    std::vector<codegen::Value> aggregate_vals;
    aggregation_.FinalizeValues(GetCodeGen(), LoadStatePtr(mat_buffer_id_),
                                aggregate_vals);

    // Collect accessors for each aggregate
    const auto &plan = GetPlanAs<planner::AggregatePlan>();

    std::vector<BufferAttributeAccess> buffer_accessors;
    const auto &agg_terms = plan.GetUniqueAggTerms();
    PELOTON_ASSERT(agg_terms.size() == aggregate_vals.size());
    for (size_t i = 0; i < agg_terms.size(); i++) {
      buffer_accessors.emplace_back(aggregate_vals, i);
    }
    for (size_t i = 0; i < agg_terms.size(); i++) {
      batch.AddAttribute(&agg_terms[i].agg_ai, &buffer_accessors[i]);
    }

    // Create a new consumer context, send (single row) batch to parent
    ctx.Consume(batch);
  };

  // Run the last pipeline serially
  GetPipeline().RunSerial(producer);
}

void GlobalGroupByTranslator::Consume(ConsumerContext &,
                                      RowBatch::Row &row) const {
  // Get the updates to advance the aggregates
  const auto &plan = GetPlanAs<planner::AggregatePlan>();

  auto &aggregates = plan.GetUniqueAggTerms();
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
void GlobalGroupByTranslator::TearDownQueryState() {
  aggregation_.TearDownQueryState(GetCodeGen());
}

}  // namespace codegen
}  // namespace peloton