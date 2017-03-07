//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// global_group_by_translator.h
//
// Identification: src/include/codegen/global_group_by_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/aggregation.h"
#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"
#include "codegen/pipeline.h"
#include "planner/aggregate_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A global group-by is when only a single (global) group is produced as output.
// Another way to think of it is a SQL statement with aggregates, but without
// a group-by clause.
//===----------------------------------------------------------------------===//
class GlobalGroupByTranslator : public OperatorTranslator {
 public:
  //===--------------------------------------------------------------------===//
  // An accessor into a single tuple stored in buffered state
  //===--------------------------------------------------------------------===//
  class BufferAttributeAccess : public RowBatch::AttributeAccess {
   public:
    // Constructor
    BufferAttributeAccess(const std::vector<Value> &aggregate_vals,
                          uint32_t agg_index)
        : aggregate_vals_(aggregate_vals), agg_index_(agg_index) {}

    Value Access(CodeGen &, RowBatch::Row &) override {
      return aggregate_vals_[agg_index_];
    }

   private:
    // All the vals
    const std::vector<codegen::Value> &aggregate_vals_;
    // The value this accessor is for
    uint32_t agg_index_;
  };

  // Constructor
  GlobalGroupByTranslator(const planner::AggregatePlan &plan,
                          CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  void DefineFunctions() override {}

  void Produce() const override;

  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  void TearDownState() override {}

  std::string GetName() const override;

 private:
  // The aggregation plan
  const planner::AggregatePlan &plan_;

  // The pipeline the child operator of this aggregation belongs to
  Pipeline child_pipeline_;

  // The class responsible for handling the aggregation for all our aggregates
  Aggregation aggregation_;

  // The ID of our materialization buffer in the runtime state
  RuntimeState::StateID mat_buffer_id_;

  // The ID of our output vector in the runtime state
  RuntimeState::StateID output_vector_id_;
};

}  // namespace codegen
}  // namespace peloton