//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compilation_context.cpp
//
// Identification: src/codegen/compilation_context.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compilation_context.h"

#include "codegen/proxy/storage_manager_proxy.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "codegen/proxy/query_parameters_proxy.h"
#include "codegen/proxy/storage_manager_proxy.h"
#include "common/logger.h"
#include "common/timer.h"

namespace peloton {
namespace codegen {

namespace {

// Reset the cache (if already loaded), the populate the cache
void InitializeParameterCache(CodeGen &codegen, ParameterCache &cache,
                              llvm::Value *runtime_query_parameters) {
  cache.Reset();
  cache.Populate(codegen, runtime_query_parameters);
}

}  // anonymous namespace

// Constructor
CompilationContext::CompilationContext(Query &query,
                                       const QueryParametersMap &parameters_map,
                                       QueryResultConsumer &result_consumer)
    : query_(query),
      parameters_map_(parameters_map),
      parameter_cache_(parameters_map_),
      result_consumer_(result_consumer),
      codegen_(query_.GetCodeContext()) {
  // Allocate a storage manager instance in the runtime state
  auto &runtime_state = GetRuntimeState();

  auto *storage_manager_ptr_type =
      StorageManagerProxy::GetType(codegen_)->getPointerTo();
  storage_manager_state_id_ =
      runtime_state.RegisterState("storageManager", storage_manager_ptr_type);

  auto *executor_context_type =
      ExecutorContextProxy::GetType(codegen_)->getPointerTo();
  executor_context_state_id_ =
      runtime_state.RegisterState("executorContext", executor_context_type);

  auto *query_parameters_type =
      QueryParametersProxy::GetType(codegen_)->getPointerTo();
  query_parameters_state_id_ =
      runtime_state.RegisterState("queryParameters", query_parameters_type);

  // Let the query consumer modify the runtime state object
  result_consumer_.Prepare(*this);
}

// Prepare the translator for the given operator
void CompilationContext::Prepare(const planner::AbstractPlan &op,
                                 Pipeline &pipeline) {
  auto translator = translator_factory_.CreateTranslator(op, *this, pipeline);
  op_translators_.insert(std::make_pair(&op, std::move(translator)));
}

// Prepare the translator for the given expression
void CompilationContext::Prepare(const expression::AbstractExpression &exp) {
  auto translator = translator_factory_.CreateTranslator(exp, *this);
  exp_translators_.insert(std::make_pair(&exp, std::move(translator)));
}

// Produce tuples for the given operator
void CompilationContext::Produce(const planner::AbstractPlan &op) {
  auto *translator = GetTranslator(op);
  PL_ASSERT(translator != nullptr);
  translator->Produce();
}

// Generate all plan functions for the given query
void CompilationContext::GeneratePlan(QueryCompiler::CompileStats *stats) {
  // Start timing
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // First we prepare the translators for all the operators in the tree
  Prepare(query_.GetPlan(), main_pipeline_);

  if (stats != nullptr) {
    timer.Stop();
    stats->setup_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  LOG_TRACE("Main pipeline: %s", main_pipeline_.GetInfo().c_str());

  // Generate the helper functions the query needs
  GenerateHelperFunctions();

  // Generate the init() function
  llvm::Function *init = GenerateInitFunction();

  // Generate the plan() function
  llvm::Function *plan = GeneratePlanFunction(query_.GetPlan());

  // Generate the  tearDown() function
  llvm::Function *tear_down = GenerateTearDownFunction();

  if (stats != nullptr) {
    timer.Stop();
    stats->ir_gen_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Next, we prepare the query statement with the functions we've generated
  Query::QueryFunctions funcs = {init, plan, tear_down};
  bool prepared = query_.Prepare(funcs);
  if (!prepared) {
    throw Exception{"There was an error preparing the compiled query"};
  }

  // We're done
  if (stats != nullptr) {
    timer.Stop();
    stats->jit_ms = timer.GetDuration();
  }
}

// Generate any helper functions that the query needs
void CompilationContext::GenerateHelperFunctions() {
  // Allow each operator to initialize its state
  for (auto &iter : op_translators_) {
    auto &translator = iter.second;
    translator->DefineAuxiliaryFunctions();
  }

  // Define each auxiliary producer function
  auto &cc = query_.GetCodeContext();
  for (auto &iter : auxiliary_producers_) {
    const auto &plan = *iter.first;
    const auto &function_declaration = iter.second;
    FunctionBuilder func{cc, function_declaration};
    {
      // Don't try to optimize this by moving the cache population outside the
      // function definition. We need the call to exist within the context of
      // the function we're generating.
      InitializeParameterCache(codegen_, parameter_cache_,
                               GetQueryParametersPtr());
      // Let the plan produce
      Produce(plan);
      // That's it
      func.ReturnAndFinish();
    }
  }
}

// Get the storage manager pointer from the runtime state
llvm::Value *CompilationContext::GetStorageManagerPtr() {
  return GetRuntimeState().LoadStateValue(codegen_, storage_manager_state_id_);
}

llvm::Value *CompilationContext::GetExecutorContextPtr() {
  return GetRuntimeState().LoadStateValue(codegen_, executor_context_state_id_);
}

llvm::Value *CompilationContext::GetQueryParametersPtr() {
  return GetRuntimeState().LoadStateValue(codegen_, query_parameters_state_id_);
}

// Generate code for the init() function of the query
llvm::Function *CompilationContext::GenerateInitFunction() {
  // Create function definition
  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  std::string name = StringUtil::Format("_%lu_init", code_context.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}};
  FunctionBuilder init_func{code_context, name, codegen_.VoidType(), args};
  {
    // Let the consumer initialize
    result_consumer_.InitializeState(*this);

    // Allow each operator to initialize their state
    for (auto &iter : op_translators_) {
      auto &translator = iter.second;
      translator->InitializeState();
    }

    // Finish the function
    init_func.ReturnAndFinish();
  }

  // Get the generated function
  return init_func.GetFunction();
}

// Generate the code for the plan() function of the query
llvm::Function *CompilationContext::GeneratePlanFunction(
    const planner::AbstractPlan &root) {
  // Create function definition
  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  std::string name = StringUtil::Format("_%lu_plan", code_context.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}};
  FunctionBuilder plan_func{code_context, name, codegen_.VoidType(), args};
  {
    // Load the query parameter values
    InitializeParameterCache(codegen_, parameter_cache_,
                             GetQueryParametersPtr());

    // Generate the primary plan logic
    Produce(root);

    // Finish the function
    plan_func.ReturnAndFinish();
  }

  // Get the function
  return plan_func.GetFunction();
}

// Generate the code for the tearDown() function of the query
llvm::Function *CompilationContext::GenerateTearDownFunction() {
  // Create function definition
  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  std::string name = StringUtil::Format("_%lu_tearDown", code_context.GetID());
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}};
  FunctionBuilder tear_down_func{code_context, name, codegen_.VoidType(), args};
  {
    // Let the consumer cleanup
    result_consumer_.TearDownState(*this);

    // Allow each operator to clean up their state
    for (auto &iter : op_translators_) {
      auto &translator = iter.second;
      translator->TearDownState();
    }

    // Finish the function
    tear_down_func.ReturnAndFinish();
  }
  // Get the function
  return tear_down_func.GetFunction();
}

// Get the registered translator for the given expression
ExpressionTranslator *CompilationContext::GetTranslator(
    const expression::AbstractExpression &exp) const {
  auto iter = exp_translators_.find(&exp);
  return iter == exp_translators_.end() ? nullptr : iter->second.get();
}

// Get the registered translator for the given operator
OperatorTranslator *CompilationContext::GetTranslator(
    const planner::AbstractPlan &op) const {
  auto iter = op_translators_.find(&op);
  return iter == op_translators_.end() ? nullptr : iter->second.get();
}

AuxiliaryProducerFunction CompilationContext::DeclareAuxiliaryProducer(
    const planner::AbstractPlan &plan, const std::string &provided_name) {
  auto iter = auxiliary_producers_.find(&plan);
  if (iter != auxiliary_producers_.end()) {
    const auto &declaration = iter->second;
    return AuxiliaryProducerFunction(declaration);
  }

  auto &cc = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  // Make the declaration for the caller to use

  std::string fn_name;
  if (!provided_name.empty()) {
    fn_name = provided_name;
  } else {
    fn_name = StringUtil::Format("_%lu_auxPlanFunction", cc.GetID());
  }

  std::vector<FunctionDeclaration::ArgumentInfo> fn_args = {
      {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}};

  auto declaration = FunctionDeclaration::MakeDeclaration(
      cc, fn_name, FunctionDeclaration::Visibility::Internal,
      codegen_.VoidType(), fn_args);

  // Save the function declaration for later definition
  auxiliary_producers_.emplace(&plan, declaration);

  return AuxiliaryProducerFunction(declaration);
}

}  // namespace codegen
}  // namespace peloton
