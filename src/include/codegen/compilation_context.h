//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compilation_context.h
//
// Identification: src/include/codegen/compilation_context.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>

#include "codegen/code_context.h"
#include "codegen/codegen.h"
#include "codegen/runtime_state.h"
#include "codegen/expression_translator.h"
#include "codegen/operator_translator.h"
#include "codegen/query_compiler.h"
#include "codegen/query_statement.h"
#include "codegen/translator_factory.h"
#include "expression/abstract_expression.h"
#include "planner/abstract_plan.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// All the state for the current compilation unit (i.e., one query). This state
// includes translations for every operator in the tree, the context where all
// the code is produced, the runtime state that tracks all the runtime objects
// that the query needs, and the consumer of the results. The main entry point
// is ConstructPlan() that takes in the query plan and a stats object to track
// compile-time statistics.
//===----------------------------------------------------------------------===//
class CompilationContext {
  friend class ConsumerContext;

 public:
  // Constructor
  CompilationContext(CodeContext &code_context, RuntimeState &runtime_state,
                     TranslatorFactory &factory,
                     QueryResultConsumer &result_consumer);

  // Prepare a translator in this context
  void Prepare(const planner::AbstractPlan &op, Pipeline &pipeline);
  void Prepare(const expression::AbstractExpression &expression);

  // Produce the tuples for the given operator
  void Produce(const planner::AbstractPlan &op);

  // This is the main entry point into the compilation component. Callers
  // construct a compilation context, then invoke this method to compile
  // the plan and prepare the provided query statement.
  void GeneratePlan(QueryStatement &query, QueryCompiler::CompileStats *stats);

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  CodeGen &GetCodeGen() { return codegen_; }

  RuntimeState &GetRuntimeState() const { return runtime_state_; }

  QueryResultConsumer &GetQueryResultConsumer() const {
    return result_consumer_;
  }

  llvm::Value *GetCatalogPtr();

 private:
  // Generate any helper functions that the query needs (i.e. any auxillary
  // functions)
  void GenerateHelperFunctions();

  // Generate the init() function of the query
  llvm::Function *GenerateInitFunction();

  // Generate the produce() function of the query
  llvm::Function *GeneratePlanFunction(const planner::AbstractPlan &root);

  // Generate the tearDown() function of the query
  llvm::Function *GenerateTearDownFunction();

  // Get the registered translator for the given operator/expression
  ExpressionTranslator *GetTranslator(
      const expression::AbstractExpression &exp) const;
  OperatorTranslator *GetTranslator(const planner::AbstractPlan &op) const;

 private:
  // The code context (i.e., where all the code goes)
  CodeContext &code_context_;

  // The code generator
  CodeGen codegen_;

  // The main pipeline
  Pipeline main_pipeline_;

  // The runtime state
  RuntimeState &runtime_state_;

  // The factory that creates translators for operators and expressions
  TranslatorFactory &translator_factory_;

  // The consumer of the results of the query
  QueryResultConsumer &result_consumer_;

  // The ID for the database/manager instance in the runtime state
  RuntimeState::StateID catalog_state_id_;

  // The mapping of an operator in the tree to its translator
  std::unordered_map<const planner::AbstractPlan *,
                     std::unique_ptr<OperatorTranslator>> op_translators_;

  // The mapping of an expression somewhere in the tree to its translator
  std::unordered_map<const expression::AbstractExpression *,
                     std::unique_ptr<ExpressionTranslator>> exp_translators_;
};

}  // namespace codegen
}  // namespace peloton