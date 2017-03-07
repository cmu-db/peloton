//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// operator_translator.h
//
// Identification: src/include/codegen/operator_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
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
// Produce() and Consume() methods.  The former generates code that produces new
// tuples and the latter generates code to operate on available attributes in
// the provided ConsumerContext parameter argument.
//
// Translators are given the chance to introduce state into execution by calling
// into the RuntimeState object. Operators request slots in the execution
// state for arbitrary pieces of state, and can initialize their state in the
// InitializeState() method. Similarly, the TearDownState() method can be used
// to clean up and destroy any allocated state.
//
// Translators are also allowed to declare helper functions. These functions
// must be defined and implemented in the DefineFunctions() method, which is
// guaranteed to be called on all operators before any other method.
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
  virtual void DefineFunctions() = 0;

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
  llvm::Value *GetCatalogPtr() const;

  // Retrieve a parameter from the runtime state
  llvm::Value *GetStatePtr(const RuntimeState::StateID &state_id) const;
  llvm::Value *GetStateValue(const RuntimeState::StateID &state_id) const;

 private:
  // The compilation state context
  CompilationContext &context_;

  // The pipeline this translator is a part of
  Pipeline &pipeline_;
};

}  // namespace peloton
}  // namespace codegen