//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.h
//
// Identification: src/include/codegen/operator/operator_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/pipeline.h"
#include "codegen/row_batch.h"
#include "codegen/runtime_state.h"

namespace peloton {

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

// Forward declare
class CompilationContext;
class ConsumerContext;

//===----------------------------------------------------------------------===//
//
// The base class of all operator translators.
//
// State:
// ------
// Operators may require state in order to operate. State required for the
// duration of the whole query is declared and registered with RuntimeState in
// CompilationContext. Query state must be initialized in
// InitializeQueryState() and cleaned up in TearDownQueryState(), both of which
// are guaranteed to be called at most once. Translators may not assume an
// initialization order.
//
// Data-flow:
// ----------
// Translators implement the push-based data-flow model through definitions of
// Produce() and Consume(). A call to Produce() is a request to begin producing
// a batch of rows (i.e., a RowBatch). A call to Consume() can be viewed as a
// call-back from its child to implement logic to operate on the given set
// or rows, or an individual row.
//
// Helper functions:
// -----------------
// Translators are also allowed to declare helper functions. These functions
// must be defined and implemented in the DefineAuxiliaryFunctions() method,
// which is guaranteed to be called on all operators before any other method.
//
//===----------------------------------------------------------------------===//
class OperatorTranslator {
 public:
  /// Constructor
  OperatorTranslator(const planner::AbstractPlan &plan,
                     CompilationContext &context, Pipeline &pipeline);

  /// Destructor
  virtual ~OperatorTranslator() = default;

  /// Codegen any initialization work for this operator
  virtual void InitializeQueryState() = 0;

  /// Codegen any initialization at the start of the given pipeline
  virtual void DeclarePipelineState(PipelineContext &pipeline_context);
  virtual void InitializePipelineState(PipelineContext &pipeline_context);

  /// Define any helper functions this translator needs
  virtual void DefineAuxiliaryFunctions() = 0;

  /// The method that produces new tuples
  virtual void Produce() const = 0;

  /// The method that consumes tuples from child operators
  virtual void Consume(ConsumerContext &context, RowBatch &batch) const;
  virtual void Consume(ConsumerContext &context, RowBatch::Row &row) const = 0;

  /// Codegen any cleanup work for this translator
  virtual void TearDownQueryState() = 0;

  /// Return the plan node this translator is for
  const planner::AbstractPlan &GetPlan() const { return plan_; }

  /// Return the compilation context
  CompilationContext &GetCompilationContext() const { return context_; }

 protected:
  template <typename T>
  const T &GetPlanAs() const {
    return static_cast<const T &>(plan_);
  }

  // Return the pipeline the operator is a part of
  Pipeline &GetPipeline() const { return pipeline_; }

  // Return the code generator
  CodeGen &GetCodeGen() const;

  // Load pointers to the ExecutorContext, Transaction, or StorageManager
  llvm::Value *GetExecutorContextPtr() const;
  llvm::Value *GetTransactionPtr() const;
  llvm::Value *GetStorageManagerPtr() const;

  // Retrieve a parameter from the runtime state
  llvm::Value *LoadStatePtr(const RuntimeState::StateID &state_id) const;
  llvm::Value *LoadStateValue(const RuntimeState::StateID &state_id) const;

 private:
  // The plan node
  const planner::AbstractPlan &plan_;

  // The compilation state context
  CompilationContext &context_;

  // The pipeline this translator is a part of
  Pipeline &pipeline_;
};

}  // namespace peloton
}  // namespace codegen
