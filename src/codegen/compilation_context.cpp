//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// compilation_context.cpp
//
// Identification: src/codegen/compilation_context.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/compilation_context.h"

#include "codegen/manager_proxy.h"
#include "common/timer.h"

namespace peloton {
namespace codegen {

// Constructor
CompilationContext::CompilationContext(CodeContext &code_context,
                                       RuntimeState &runtime_state,
                                       TranslatorFactory &factory,
                                       QueryResultConsumer &result_consumer)
    : code_context_(code_context),
      codegen_(code_context),
      runtime_state_(runtime_state),
      translator_factory_(factory),
      result_consumer_(result_consumer) {
  // Allocate a manager instance in the runtime state
  auto *manager_ptr_type = ManagerProxy::GetType(codegen_)->getPointerTo();
  manager_state_id_ = runtime_state_.RegisterState("manager", manager_ptr_type);

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
  // Get the appropriate translator for the operator, then get it to produce
  // tuples dammit!
  auto *translator = GetTranslator(op);
  assert(translator != nullptr);
  translator->Produce();
}

// Generate a JITed plan for the given query
void CompilationContext::GeneratePlan(QueryStatement &query,
                                      QueryCompiler::CompileStats *stats) {
  // Start timing
  Timer<std::ratio<1, 1000>> timer;
  timer.Start();

  // First we prepare the translators for all the operators in the tree
  Pipeline pipeline;
  Prepare(query.GetPlan(), pipeline);

  if (stats) {
    timer.Stop();
    stats->setup_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  LOG_DEBUG("Main pipeline: %s", pipeline.GetInfo().c_str());

  // Generate the helper functions the query needs
  GenerateHelperFunctions();

  // Generate the init() function
  llvm::Function *init = GenerateInitFunction();

  // Generate the plan() function
  llvm::Function *plan = GeneratePlanFunction(query.GetPlan());

  // Generate the  tearDown() function
  llvm::Function *tear_down = GenerateTearDownFunction();

  if (stats != nullptr) {
    timer.Stop();
    stats->ir_gen_ms = timer.GetDuration();
    timer.Reset();
    timer.Start();
  }

  // Get the size of the runtime state parameter that is passed around
  llvm::Type *state_type = runtime_state_.ConstructType(codegen_);
  const llvm::DataLayout &data_layout =
      code_context_.GetModule().getDataLayout();
  uint64_t state_size = data_layout.getTypeAllocSize(state_type);

  // Next, we setup the query statement with the functions we've generated so
  // it can JIT compile them
  query.Setup(state_size, init, plan, tear_down);
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
    translator->DefineFunctions();
  }
}

// Get the manager pointer from the runtime state
llvm::Value *CompilationContext::GetManagerPtr() {
  return runtime_state_.GetStateValue(codegen_, manager_state_id_);
}

// Generate code for the init() function of the query. All that happens is the
// creation of the function definition whose body is the concatenation of each
// of the translators' InitializeState() calls.
llvm::Function *CompilationContext::GenerateInitFunction() {
  // Create function definition
  auto init_fn_name = "_" + std::to_string(code_context_.GetID()) + "_init";
  FunctionBuilder function_builder{
      code_context_,
      init_fn_name,
      codegen_.VoidType(),
      {{"runtimeState",
        runtime_state_.ConstructType(codegen_)->getPointerTo()}}};

  // Let the consumer initialize
  result_consumer_.InitializeState(*this);

  // Allow each operator to initialize its state
  for (auto &iter : op_translators_) {
    auto &translator = iter.second;
    translator->InitializeState();
  }

  // Finish the function
  function_builder.ReturnAndFinish();

  // Get the function
  return function_builder.GetFunction();
}

// Generate the code for the plan() function of the query. This is the main
// workhorse function. It executes the produce/consume logic for the query plan.
llvm::Function *CompilationContext::GeneratePlanFunction(
    const planner::AbstractPlan &root) {
  // Create function definition
  auto plan_fn_name = "_" + std::to_string(code_context_.GetID()) + "_plan";
  FunctionBuilder function_builder{
      code_context_,
      plan_fn_name,
      codegen_.VoidType(),
      {{"runtimeState",
        runtime_state_.ConstructType(codegen_)->getPointerTo()}}};

  // Let the result consumer prepare any state before the generation of the plan
  result_consumer_.PrepareResult(*this);

  // Create all local state
  runtime_state_.CreateLocalState(codegen_);

  // Generate the primary plan logic
  Produce(root);

  // Finish the function
  function_builder.ReturnAndFinish();

  // Get the function
  return function_builder.GetFunction();
}

// Generate the code for the tearDown() function of the query
llvm::Function *CompilationContext::GenerateTearDownFunction() {
  // Create function definition
  auto fn_name = "_" + std::to_string(code_context_.GetID()) + "_tearDown";
  FunctionBuilder function_builder{
      code_context_,
      fn_name,
      codegen_.VoidType(),
      {{"runtimeState",
        runtime_state_.ConstructType(codegen_)->getPointerTo()}}};

  // Let the consumer initialize
  result_consumer_.TearDownState(*this);

  // Allow each operator to initialize its state
  for (auto &iter : op_translators_) {
    auto &translator = iter.second;
    translator->TearDownState();
  }

  // Finish the function
  function_builder.ReturnAndFinish();

  // Get the function
  return function_builder.GetFunction();
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

}  // namespace codegen
}  // namespace peloton