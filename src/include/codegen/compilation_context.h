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
#include "codegen/query.h"
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
// All the state for the current compilation unit (i.e., one query). This state
// includes translations for every operator and expression in the tree, the
// context where all the code is produced, the runtime state that tracks all the
// runtime objects that the query needs, and the consumer of the results. Users
// wishing to compile plans make a call to GeneratePlan(...) with query plan.
//===----------------------------------------------------------------------===//
class CompilationContext {
  friend class ConsumerContext;
  friend class RowBatch;

 public:
  // Constructor
  CompilationContext(Query &query, QueryResultConsumer &result_consumer);

  // Prepare a translator in this context
  void Prepare(const planner::AbstractPlan &op, Pipeline &pipeline);
  void Prepare(const expression::AbstractExpression &expression);

  // Produce the tuples for the given operator
  void Produce(const planner::AbstractPlan &op);

  // This is the main entry point into the compilation component. Callers
  // construct a compilation context, then invoke this method to compile
  // the plan and prepare the provided query statement.
  void GeneratePlan(QueryCompiler::CompileStats *stats);

  uint32_t StoreParam(Parameter param) { return query_.StoreParam(param); }

  void StoreTargetList(TargetList &target_list) { query_.StoreTargetList(target_list); }

  void StoreDirectList(DirectMapList &direct_list) { query_.StoreDirectList(direct_list); }

  //===--------------------------------------------------------------------===//
  // ACCESSORS
  //===--------------------------------------------------------------------===//

  CodeGen &GetCodeGen() { return codegen_; }

  RuntimeState &GetRuntimeState() const { return query_.GetRuntimeState(); }

  QueryResultConsumer &GetQueryResultConsumer() const {
    return result_consumer_;
  }

  // Get a pointer to the catalog object from the runtime state
  llvm::Value *GetCatalogPtr();

  // Get a pointer to the transaction object from runtime state
  llvm::Value *GetTransactionPtr();

  // Get a pointer to the serialized param (in context of parameterization) instances
  llvm::Value *GetCharPtrParamPtr();
  // Get a pointer to the serialized param (in context of parameterization) instance sizes
  llvm::Value *GetCharLenParamPtr();

  // Get a pointer to the target list instance
  llvm::Value *GetTargetListPtr();
  // Get a pointer to the direct list instance
  llvm::Value *GetDirectListPtr();

  // Get a pointer to the executor context instance
  llvm::Value *GetExecContextPtr();

 private:
  // Generate any auxiliary helper functions that the query needs
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
  // The query we'll compile
  Query &query_;

  // The consumer of the results of the query
  QueryResultConsumer &result_consumer_;

  // The code generator
  CodeGen codegen_;

  // The main pipeline
  Pipeline main_pipeline_;

  // The factory that creates translators for operators and expressions
  TranslatorFactory translator_factory_;

  // The ID for the catalog and transaction state
  RuntimeState::StateID txn_state_id_;
  RuntimeState::StateID catalog_state_id_;
  RuntimeState::StateID char_ptr_params_state_id_;
  RuntimeState::StateID char_len_params_state_id_;
  RuntimeState::StateID target_list_state_id_;
  RuntimeState::StateID direct_list_state_id_;
  RuntimeState::StateID exec_context_state_id_;

  // The mapping of an operator in the tree to its translator
  std::unordered_map<const planner::AbstractPlan *,
                     std::unique_ptr<OperatorTranslator>> op_translators_;

  // The mapping of an expression somewhere in the tree to its translator
  std::unordered_map<const expression::AbstractExpression *,
                     std::unique_ptr<ExpressionTranslator>> exp_translators_;
};

}  // namespace codegen
}  // namespace peloton