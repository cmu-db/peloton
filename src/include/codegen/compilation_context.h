//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compilation_context.h
//
// Identification: src/include/codegen/compilation_context.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_map>
#include <vector>

#include "codegen/auxiliary_producer_function.h"
#include "codegen/code_context.h"
#include "codegen/codegen.h"
#include "codegen/expression/expression_translator.h"
#include "codegen/function_builder.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/query.h"
#include "codegen/query_compiler.h"
#include "codegen/query_state.h"
#include "codegen/translator_factory.h"

namespace peloton {

namespace expression {
class AbstractExpression;
}  // namespace expression

namespace planner {
class AbstractPlan;
}  // namespace planner

namespace codegen {

//===----------------------------------------------------------------------===//
//
// All the state for the current compilation unit (i.e., one query). This state
// includes translations for every operator and expression in the tree, the
// context where all the code is produced, the runtime state that tracks all the
// runtime objects that the query needs, and the consumer of the results. Users
// wishing to compile plans make a call to GeneratePlan(...) with query plan.
//
//===----------------------------------------------------------------------===//
class CompilationContext {
 public:
  /// Constructor
  CompilationContext(CodeContext &code, QueryState &query_state,
                     const QueryParametersMap &parameters_map,
                     ExecutionConsumer &execution_consumer);

  /// This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(CompilationContext);

  /// Prepare a translator in this context
  void Prepare(const planner::AbstractPlan &op, Pipeline &pipeline);
  void Prepare(const expression::AbstractExpression &expression);

  /// Produce the tuples for the given operator
  void Produce(const planner::AbstractPlan &op);

  /// This is the main entry point into the compilation component. Callers
  /// construct a compilation context, then invoke this method to compile
  /// the plan and prepare the provided query statement.
  void GeneratePlan(Query &query, QueryCompiler::CompileStats *stats);

  /// Declare an extra function that produces tuples outside of the main plan
  /// function. The primary producer in this function is the provided plan node.
  AuxiliaryProducerFunction DeclareAuxiliaryProducer(
      const planner::AbstractPlan &plan, const std::string &provided_name);

  /// Register the given pipeline in this context
  uint32_t RegisterPipeline(Pipeline &pipeline) {
    auto pos = static_cast<uint32_t>(pipelines_.size());
    pipelines_.push_back(&pipeline);
    return pos;
  }

  bool IsLastPipeline(const Pipeline &p) const { return p.GetId() == 0; }

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Accessors
  ///
  //////////////////////////////////////////////////////////////////////////////

  CodeGen &GetCodeGen() { return codegen_; }

  QueryState &GetQueryState() { return query_state_; }

  ParameterCache &GetParameterCache() { return parameter_cache_; }

  ExecutionConsumer &GetExecutionConsumer() const { return exec_consumer_; }

  ExpressionTranslator *GetTranslator(
      const expression::AbstractExpression &exp) const;

  OperatorTranslator *GetTranslator(const planner::AbstractPlan &op) const;

 private:
  // Generate any auxiliary helper functions that the query needs
  void GenerateHelperFunctions();

  // Generate the init() function of the query
  llvm::Function *GenerateInitFunction();

  // Generate the produce() function of the query
  llvm::Function *GeneratePlanFunction(const planner::AbstractPlan &root);

  // Generate the tearDown() function of the query
  llvm::Function *GenerateTearDownFunction();

 private:
  // The context where all the code lives
  CodeContext &code_context_;

  // Runtime state
  QueryState &query_state_;

  // The parameter value cache of the query
  ParameterCache parameter_cache_;

  // The consumer of the results of the query
  ExecutionConsumer &exec_consumer_;

  // The code generator
  CodeGen codegen_;

  // All the pipelines
  std::vector<Pipeline *> pipelines_;

  // The main pipeline
  Pipeline main_pipeline_;

  // The factory that creates translators for operators and expressions
  TranslatorFactory translator_factory_;

  // The mapping of an operator in the tree to its translator
  std::unordered_map<const planner::AbstractPlan *,
                     std::unique_ptr<OperatorTranslator>> op_translators_;

  // The mapping of an expression somewhere in the tree to its translator
  std::unordered_map<const expression::AbstractExpression *,
                     std::unique_ptr<ExpressionTranslator>> exp_translators_;

  // Pre-declared producer functions and their root plan nodes
  std::unordered_map<const planner::AbstractPlan *, FunctionDeclaration>
      auxiliary_producers_;
};

}  // namespace codegen
}  // namespace peloton
