//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.h
//
// Identification: src/include/codegen/operator/operator_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/pipeline.h"
#include "codegen/row_batch.h"
#include "codegen/runtime_state.h"

namespace peloton {
namespace codegen {

// Forward declare
class CompilationContext;
class ConsumerContext;

//===----------------------------------------------------------------------===//
// The base class of all operator translators.
//
// Translators implement the push-based data-flow model by implementing the
// Produce() and Consume() methods. State is managed through the RuntimeState
// object found in the compilation context. State can either be global (i.e.
// external) or local (i.e., on the stack and automatically cleaned up). All
// external state should be initialized in InitializeState() and cleaned up in
// TearDownState().
//
// Translators are also allowed to declare helper functions. These functions
// must be defined and implemented in the DefineAuxiliaryFunctions() method,
// which is guaranteed to be called on all operators before any other method.
//===----------------------------------------------------------------------===//
class OperatorTranslator {
 public:
  // Constructor
  OperatorTranslator(CompilationContext &context, Pipeline &pipeline);

  // Destructor
  virtual ~OperatorTranslator() {}

  // Codegen any initialization work for this operator
  virtual void InitializeState() = 0;

  // Define any helper functions this translator needs
  virtual void DefineAuxiliaryFunctions() = 0;

  // The method that produces new tuples
  virtual void Produce() const = 0;

  // The method that consumes tuples from child operators
  virtual void Consume(ConsumerContext &context, RowBatch &batch) const;
  virtual void Consume(ConsumerContext &context, RowBatch::Row &row) const = 0;

  // Codegen any cleanup work for this translator
  virtual void TearDownState() = 0;

  virtual std::string GetName() const = 0;

 protected:
  // Return the compilation context
  CompilationContext &GetCompilationContext() const { return context_; }

  // Return the pipeline the operator is a part of
  Pipeline &GetPipeline() const { return pipeline_; }

  // Return the code generator
  CodeGen &GetCodeGen() const;

  // Get the pointer to the Manager from the runtime state parameter
  llvm::Value *GetStorageManagerPtr() const;

  // Retrieve a parameter from the runtime state
  llvm::Value *LoadStatePtr(const RuntimeState::StateID &state_id) const;
  llvm::Value *LoadStateValue(const RuntimeState::StateID &state_id) const;

 private:
  // The compilation state context
  CompilationContext &context_;

  // The pipeline this translator is a part of
  Pipeline &pipeline_;
};

}  // namespace peloton
}  // namespace codegen
