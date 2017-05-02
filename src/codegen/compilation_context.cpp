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

#include "codegen/catalog_proxy.h"
#include "codegen/multi_thread_context.h"
#include "codegen/multi_thread_context_proxy.h"
#include "codegen/transaction_proxy.h"
#include "common/logger.h"
#include "common/timer.h"

#include "codegen/query_thread_pool_proxy.h"
#include "codegen/barrier.h"
#include "codegen/barrier_proxy.h"

namespace peloton {
namespace codegen {

// Constructor
CompilationContext::CompilationContext(Query &query,
                                       QueryResultConsumer &result_consumer)
    : query_(query),
      result_consumer_(result_consumer),
      codegen_(query_.GetCodeContext()) {
  // Allocate a catalog and transaction instance in the runtime state
  auto &runtime_state = GetRuntimeState();

  auto *txn_type = TransactionProxy::GetType(codegen_)->getPointerTo();
  txn_state_id_ = runtime_state.RegisterState("transaction", txn_type);

  auto *catalog_ptr_type = CatalogProxy::GetType(codegen_)->getPointerTo();
  catalog_state_id_ = runtime_state.RegisterState("catalog", catalog_ptr_type);

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

  LOG_DEBUG("Main pipeline: %s", main_pipeline_.GetInfo().c_str());

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
}

// Get the catalog pointer from the runtime state
llvm::Value *CompilationContext::GetCatalogPtr() {
  return GetRuntimeState().LoadStateValue(codegen_, catalog_state_id_);
}

// Get the transaction pointer from the runtime state
llvm::Value *CompilationContext::GetTransactionPtr() {
  return GetRuntimeState().LoadStateValue(codegen_, txn_state_id_);
}

// Generate code for the init() function of the query
llvm::Function *CompilationContext::GenerateInitFunction() {
  // Create function definition
  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  auto init_fn_name = "_" + std::to_string(code_context.GetID()) + "_init";
  FunctionBuilder function_builder{
      code_context,
      init_fn_name,
      codegen_.VoidType(),
      {{"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}}};

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

// NOTE:
llvm::Function *CompilationContext::GenerateInnerPlanFunction(const planner::AbstractPlan &root) {
    // Create function definition
    auto &code_context = query_.GetCodeContext();
    auto &runtime_state = query_.GetRuntimeState();

    auto inner_plan_fn_name = "_" + std::to_string(code_context.GetID()) + "_inner_plan";
    FunctionBuilder inner_function_builder{
        code_context,
        inner_plan_fn_name,
        codegen_.VoidType(),
        {
          {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()},
          {"multiThreadContext", MultiThreadContextProxy::GetType(codegen_)->getPointerTo()}
        }};

    llvm::Value *multi_thread_context = codegen_.GetArgument(1);

    llvm::Value *thread_id = codegen_.CallFunc(
        MultiThreadContextProxy::GetThreadIdFunction(codegen_),
        {multi_thread_context});

    codegen_.CallPrintf("#%d, Inner plan start.\n", {thread_id});

    // Create all local state
    runtime_state.CreateLocalState(codegen_);

    // Generate the primary plan logic
    Produce(root);

    codegen_.CallPrintf("#%d, Inner plan end.\n", {thread_id});

    // barrier wait until all threads reach here.
    llvm::Value *barrier = codegen_.CallFunc(MultiThreadContextProxy::GetGetBarrierFunction(codegen_), {multi_thread_context});
    codegen_.CallPrintf("#%d, innerplan: barrier waiting. \n", {thread_id});
    codegen_.CallFunc(BarrierProxy::GetBarrierWaitFunction(codegen_), {barrier});
    codegen_.CallPrintf("#%d, innerplan: barrier passed. \n", {thread_id});

    // Tell barrier this thread is going to end.
    codegen_.CallFunc(BarrierProxy::GetWorkerFinishFunction(codegen_), {barrier});

    // Finish the function
    inner_function_builder.ReturnAndFinish();
    return inner_function_builder.GetFunction();
}


// Generate the code for the plan() function of the query
llvm::Function *CompilationContext::GeneratePlanFunction(
    const planner::AbstractPlan &root) {

  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();
  llvm::Function *inner_func = GenerateInnerPlanFunction(root);

  auto plan_fn_name = "_" + std::to_string(code_context.GetID()) + "_plan";
  FunctionBuilder function_builder{
        code_context,
        plan_fn_name,
        codegen_.VoidType(),
        {
          {"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}}};

  // Get runtime state information.
  llvm::Value *runtime_state_ptr = codegen_.GetState();

  // Get thread information.
  llvm::Value *thread_id = codegen_.Const64(0);
  llvm::Value *thread_count = codegen_.CallFunc(QueryThreadPoolProxy::GetThreadCountFunction(codegen_), {});
  llvm::Value *query_thread_pool = codegen_.CallFunc(QueryThreadPoolProxy::GetGetIntanceFunction(codegen_), {});

  // Get barrier information.
  llvm::Value *barrier = codegen_->CreateAlloca(BarrierProxy::GetType(codegen_));
  codegen_.CallFunc(BarrierProxy::GetInitInstanceFunction(codegen_), {barrier, thread_count});

  codegen_.CallPrintf("Start to submit threads for inner plan.\n", {});

  Loop loop{codegen_, codegen_->CreateICmpULT(thread_id, thread_count),
    {
        {"threadId", thread_id}
    }};
  {
    thread_id = loop.GetLoopVar(0);

    llvm::Value *multi_thread_context = codegen_->CreateAlloca(MultiThreadContextProxy::GetType(codegen_));
    codegen_.CallFunc(MultiThreadContextProxy::InitInstanceFunction(codegen_), {multi_thread_context, thread_id, thread_count, barrier});

    //NOTE: single-threaded
    // codegen_.CallFunc(inner_func, {runtime_state_ptr, multi_thread_context});

    //NOTE: multi-threaded
    codegen_.CallFunc(QueryThreadPoolProxy::GetSubmitQueryTaskFunction(codegen_, &runtime_state), {query_thread_pool, runtime_state_ptr, multi_thread_context, inner_func});

    // Move to next thread id in loop.
    thread_id = codegen_->CreateAdd(thread_id, codegen_.Const64(1));
    loop.LoopEnd(codegen_->CreateICmpULT(thread_id, thread_count), {thread_id});
  }

  // TODO: barrier for main thread
  codegen_.CallPrintf("Main: barrier wait. \n", {});
  codegen_.CallFunc(BarrierProxy::GetMasterWaitFunction(codegen_),{barrier});
  codegen_.CallPrintf("Main: barrier pass. \n", {});

  function_builder.ReturnAndFinish();

  return function_builder.GetFunction();
}

// Generate the code for the tearDown() function of the query
llvm::Function *CompilationContext::GenerateTearDownFunction() {
  // Create function definition
  auto &code_context = query_.GetCodeContext();
  auto &runtime_state = query_.GetRuntimeState();

  auto fn_name = "_" + std::to_string(code_context.GetID()) + "_tearDown";
  FunctionBuilder function_builder{
      code_context,
      fn_name,
      codegen_.VoidType(),
      {{"runtimeState", runtime_state.FinalizeType(codegen_)->getPointerTo()}}};

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
