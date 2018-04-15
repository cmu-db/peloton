//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pipeline.cpp
//
// Identification: src/codegen/pipeline.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pipeline.h"

#include "codegen/code_context.h"
#include "codegen/codegen.h"
#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/lang/loop.h"
#include "codegen/proxy/executor_context_proxy.h"
#include "settings/settings_manager.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// LoopOverStates
///
////////////////////////////////////////////////////////////////////////////////

PipelineContext::LoopOverStates::LoopOverStates(PipelineContext &pipeline_ctx)
    : ctx_(pipeline_ctx) {}

void PipelineContext::LoopOverStates::Do(
    const std::function<void(llvm::Value *)> &body) const {
  auto &compilation_ctx = ctx_.GetPipeline().GetCompilationContext();
  auto &exec_consumer = compilation_ctx.GetExecutionConsumer();
  auto *thread_states = exec_consumer.GetThreadStatesPtr(compilation_ctx);

  CodeGen &codegen = compilation_ctx.GetCodeGen();

  llvm::Value *num_threads =
      codegen.Load(ThreadStatesProxy::num_threads, thread_states);
  llvm::Value *state_size =
      codegen.Load(ThreadStatesProxy::state_size, thread_states);
  llvm::Value *states = codegen.Load(ThreadStatesProxy::states, thread_states);

  llvm::Value *state_end = codegen->CreateInBoundsGEP(
      states, {codegen->CreateMul(num_threads, state_size)});

  llvm::Value *loop_cond = codegen->CreateICmpNE(states, state_end);
  lang::Loop state_loop{codegen, loop_cond, {{"threadState", states}}};
  {
    // Pull out state in this iteration
    llvm::Value *curr_state = state_loop.GetLoopVar(0);
    // Invoke caller
    body(curr_state);
    // Wrap up
    states = codegen->CreateInBoundsGEP(states, {state_size});
    state_loop.LoopEnd(codegen->CreateICmpNE(states, state_end), {states});
  }
}

////////////////////////////////////////////////////////////////////////////////
///
/// Pipeline context
///
////////////////////////////////////////////////////////////////////////////////

PipelineContext::PipelineContext(Pipeline &pipeline)
    : pipeline_(pipeline),
      init_flag_id_(0),
      thread_state_type_(nullptr),
      thread_state_(nullptr),
      thread_init_func_(nullptr),
      pipeline_func_(nullptr) {}

PipelineContext::Id PipelineContext::RegisterState(std::string name,
                                                   llvm::Type *type) {
  PELOTON_ASSERT(thread_state_type_ == nullptr);
  if (thread_state_type_ != nullptr) {
    throw Exception{"Cannot register thread state after finalization"};
  }

  PELOTON_ASSERT(state_components_.size() <
                 std::numeric_limits<uint8_t>::max());
  auto slot = static_cast<uint8_t>(state_components_.size());
  state_components_.emplace_back(name, type);
  return slot;
}

void PipelineContext::FinalizeState(CodeGen &codegen) {
  // Quit early if we've already finalized the type
  if (thread_state_type_ != nullptr) {
    return;
  }

  // Tag on the initialization flag at the end
  init_flag_id_ = RegisterState("initialized", codegen.BoolType());

  // Pull out types
  std::vector<llvm::Type *> types;
  for (const auto &slot_info : state_components_) {
    types.push_back(slot_info.second);
  }

  // Build
  thread_state_type_ =
      llvm::StructType::create(codegen.GetContext(), types, "ThreadState");
}

llvm::Value *PipelineContext::AccessThreadState(
    UNUSED_ATTRIBUTE CodeGen &codegen) const {
  PELOTON_ASSERT(thread_state_ != nullptr);
  return thread_state_;
}

llvm::Value *PipelineContext::LoadFlag(CodeGen &codegen) const {
  return LoadState(codegen, init_flag_id_);
}

void PipelineContext::StoreFlag(CodeGen &codegen, llvm::Value *flag) const {
  PELOTON_ASSERT(flag->getType()->isIntegerTy(1) &&
                 flag->getType() == codegen.BoolType());
  auto *flag_ptr = LoadStatePtr(codegen, init_flag_id_);
  codegen->CreateStore(flag, flag_ptr);
}

void PipelineContext::MarkInitialized(CodeGen &codegen) const {
  StoreFlag(codegen, codegen.ConstBool(true));
}

llvm::Value *PipelineContext::LoadStatePtr(CodeGen &codegen,
                                           PipelineContext::Id state_id) const {
  auto name = state_components_[state_id].first + "Ptr";
  return codegen->CreateConstInBoundsGEP2_32(
      GetThreadStateType(), AccessThreadState(codegen), 0, state_id, name);
}

llvm::Value *PipelineContext::LoadState(CodeGen &codegen,
                                        PipelineContext::Id state_id) const {
  auto name = state_components_[state_id].first;
  return codegen->CreateLoad(LoadStatePtr(codegen, state_id), name);
}

uint32_t PipelineContext::GetEntryOffset(CodeGen &codegen,
                                         PipelineContext::Id state_id) const {
  auto *state_type = GetThreadStateType();
  return static_cast<uint32_t>(codegen.ElementOffset(state_type, state_id));
}

bool PipelineContext::IsParallel() const { return pipeline_.IsParallel(); }

Pipeline &PipelineContext::GetPipeline() { return pipeline_; }

////////////////////////////////////////////////////////////////////////////////
///
/// Pipeline
///
////////////////////////////////////////////////////////////////////////////////

Pipeline::Pipeline(CompilationContext &compilation_ctx)
    : compilation_ctx_(compilation_ctx),
      pipeline_index_(0),
      parallelism_(Pipeline::Parallelism::Flexible) {
  id_ = compilation_ctx.RegisterPipeline(*this);
}

Pipeline::Pipeline(OperatorTranslator *translator,
                   Pipeline::Parallelism parallelism)
    : Pipeline(translator->GetCompilationContext()) {
  Add(translator, parallelism);
}

void Pipeline::Add(OperatorTranslator *translator, Parallelism parallelism) {
  // Add the operator to the pipeline
  pipeline_.push_back(translator);
  pipeline_index_ = static_cast<uint32_t>(pipeline_.size()) - 1;

  // Set parallelism
  parallelism_ = std::min(parallelism_, parallelism);
}

void Pipeline::MarkSource(OperatorTranslator *translator,
                          Pipeline::Parallelism parallelism) {
  PELOTON_ASSERT(translator == pipeline_.back());

  // Check parallel execution settings
  bool parallel_exec_disabled = !settings::SettingsManager::GetBool(
                                    settings::SettingId::parallel_execution);

  // Check if the consumer supports parallel execution
  auto &exec_consumer = compilation_ctx_.GetExecutionConsumer();
  bool parallel_consumer = exec_consumer.SupportsParallelExec();

  // We choose serial execution for one of four reasons:
  //   1. If parallel execution is globally disabled.
  //   2. If the consumer isn't parallel.
  //   3. If the source wants serial execution.
  //   4. If the pipeline is already configured to be serial.
  if (parallel_exec_disabled || !parallel_consumer ||
      parallelism == Pipeline::Parallelism::Serial ||
      parallelism_ == Pipeline::Parallelism::Serial) {
    parallelism_ = Pipeline::Parallelism::Serial;
    return;
  }

  // At this point, the pipeline is either fully parallel or flexible, and the
  // source is either parallel or flexible. We choose whatever the source wants
  if (parallelism == Pipeline::Parallelism::Flexible) {
    auto pipeline_name = ConstructPipelineName();
    auto source_name = translator->GetPlan().GetInfo();
    LOG_WARN(
        "Pipeline '%s' source (%s) chose Flexible parallelism rather "
        "than committing to Serial or Parallel. A Serial execution "
        "mode was chosen to err on the side of caution.",
        pipeline_name.c_str(), source_name.c_str());
    parallelism_ = Pipeline::Parallelism::Serial;
  } else {
    parallelism_ = Pipeline::Parallelism::Parallel;
  }
}

// Move to the next step in this pipeline
const OperatorTranslator *Pipeline::NextStep() {
  if (pipeline_index_ > 0) {
    return pipeline_[--pipeline_index_];
  } else {
    return nullptr;
  }
}

////////////////////////////////////////////////////////////////////////////////
///
/// Stage-related functionality
///
////////////////////////////////////////////////////////////////////////////////

// Install a stage boundary at the input into the given translator
void Pipeline::InstallStageBoundary(
    UNUSED_ATTRIBUTE const OperatorTranslator *translator) {
  // Validate the assumption
  PELOTON_ASSERT(pipeline_[pipeline_index_] == translator);
  stage_boundaries_.push_back(pipeline_index_ + 1);
}

bool Pipeline::AtStageBoundary() const {
  return std::find(stage_boundaries_.begin(), stage_boundaries_.end(),
                   pipeline_index_) != stage_boundaries_.end();
}

uint32_t Pipeline::GetNumStages() const {
  return static_cast<uint32_t>(stage_boundaries_.size()) + 1;
}

uint32_t Pipeline::GetTranslatorStage(
    const OperatorTranslator *translator) const {
  // If no boundaries exist, then the pipeline is composed of a single stage
  if (stage_boundaries_.empty()) {
    return 0;
  }

  uint32_t stage = 0;
  for (uint32_t pi = 0, sbi = 0; pi < pipeline_.size(); pi++) {
    // Check if we cross a stage boundary
    if (stage_boundaries_[sbi] == pi) {
      sbi++;
      stage++;
    }

    // If we've reached the translator in the pipeline, we're done
    if (pipeline_[pi] == translator) {
      break;
    }
  }
  return GetNumStages() - stage - 1;
}

////////////////////////////////////////////////////////////////////////////////
///
/// Serial/parallel execution functionality
///
////////////////////////////////////////////////////////////////////////////////

namespace {

std::string CreateUniqueFunctionName(Pipeline &pipeline,
                                     const std::string &prefix) {
  CompilationContext &compilation_ctx = pipeline.GetCompilationContext();
  CodeContext &cc = compilation_ctx.GetCodeGen().GetCodeContext();
  return StringUtil::Format("_%" PRId64 "_pipeline_%u_%s_%s", cc.GetID(),
                            pipeline.GetId(), prefix.c_str(),
                            pipeline.ConstructPipelineName().c_str());
}

}  // namespace

std::string Pipeline::ConstructPipelineName() const {
  std::vector<std::string> parts;
  for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend(); riter != rend;
       ++riter) {
    const planner::AbstractPlan &plan = (*riter)->GetPlan();
    const std::string plan_type = PlanNodeTypeToString(plan.GetPlanNodeType());
    parts.emplace_back(StringUtil::Lower(plan_type));
  }
  if (compilation_ctx_.IsLastPipeline(*this)) {
    parts.emplace_back("output");
  }
  return StringUtil::Join(parts, "_");
}

void Pipeline::InitializePipeline(PipelineContext &pipeline_ctx) {
  if (!pipeline_ctx.IsParallel()) {
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->InitializePipelineState(pipeline_ctx);
    }
    // That's it
    return;
  }

  // Let each operator in the pipeline declare state it needs
  for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend(); riter != rend;
       ++riter) {
    (*riter)->RegisterPipelineState(pipeline_ctx);
  }

  // If we're the last pipeline, let the consumer know
  ExecutionConsumer &consumer = compilation_ctx_.GetExecutionConsumer();
  if (GetCompilationContext().IsLastPipeline(*this)) {
    consumer.RegisterPipelineState(pipeline_ctx);
  }

  // Finalize thread state type
  CodeGen &codegen = compilation_ctx_.GetCodeGen();
  pipeline_ctx.FinalizeState(codegen);
  auto thread_state_size =
      static_cast<uint32_t>(codegen.SizeOf(pipeline_ctx.GetThreadStateType()));

  // Setup thread states
  llvm::Value *thread_states = consumer.GetThreadStatesPtr(compilation_ctx_);
  codegen.Call(ThreadStatesProxy::Reset,
               {thread_states, codegen.Const32(thread_state_size)});

  // Generate an initialization function
  QueryState &query_state = compilation_ctx_.GetQueryState();
  CodeContext &cc = codegen.GetCodeContext();

  auto func_name = CreateUniqueFunctionName(*this, "initializeWorkerState");
  auto visibility = FunctionDeclaration::Visibility::Internal;
  auto *ret_type = codegen.VoidType();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"queryState", query_state.GetType()->getPointerTo()},
      {"threadState", pipeline_ctx.GetThreadStateType()->getPointerTo()}};

  FunctionDeclaration init_decl(cc, func_name, visibility, ret_type, args);
  FunctionBuilder init_func(cc, init_decl);
  {
    PipelineContext::ScopedStateAccess state_access{
        pipeline_ctx, init_func.GetArgumentByPosition(1)};

    // Set initialized flag
    pipeline_ctx.MarkInitialized(codegen);

    // Let each translator initialize
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->InitializePipelineState(pipeline_ctx);
    }

    // That's it
    init_func.ReturnAndFinish();
  }
  pipeline_ctx.thread_init_func_ = init_func.GetFunction();
}

void Pipeline::CompletePipeline(PipelineContext &pipeline_ctx) {
  // Let operators in the pipeline do some post-pipeline work
  for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend(); riter != rend;
       ++riter) {
    (*riter)->FinishPipeline(pipeline_ctx);
  }

  if (!IsParallel()) {
    return;
  }

  // Loop over all states
  PipelineContext::LoopOverStates loop_state{pipeline_ctx};
  loop_state.Do([this, &pipeline_ctx](llvm::Value *thread_state) {
    PipelineContext::ScopedStateAccess state_access{pipeline_ctx, thread_state};
    // Let operators in the pipeline clean up any pipeline state
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->TearDownPipelineState(pipeline_ctx);
    }
  });
}

void Pipeline::RunSerial(const std::function<void(ConsumerContext &)> &body) {
  Run(nullptr, {}, {},
      [&body](ConsumerContext &ctx,
              UNUSED_ATTRIBUTE const std::vector<llvm::Value *> &args) {
        body(ctx);
      });
}

void Pipeline::RunParallel(
    llvm::Function *dispatch_func,
    const std::vector<llvm::Value *> &dispatch_args,
    const std::vector<llvm::Type *> &pipeline_args_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  PELOTON_ASSERT(IsParallel());
  Run(dispatch_func, dispatch_args, pipeline_args_types, body);
}

void Pipeline::Run(
    llvm::Function *dispatch_func,
    const std::vector<llvm::Value *> &dispatch_args,
    const std::vector<llvm::Type *> &pipeline_arg_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  // Create context
  PipelineContext pipeline_ctx{*this};

  // Initialize the pipeline
  InitializePipeline(pipeline_ctx);

  // Generate pipeline
  DoRun(pipeline_ctx, dispatch_func, dispatch_args, pipeline_arg_types,
        body);

  // Finish
  CompletePipeline(pipeline_ctx);
}

void Pipeline::DoRun(
    PipelineContext &pipeline_context, llvm::Function *dispatch_func,
    const std::vector<llvm::Value *> &dispatch_args,
    const std::vector<llvm::Type *> &pipeline_args_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  CodeGen &codegen = compilation_ctx_.GetCodeGen();
  QueryState &query_state = compilation_ctx_.GetQueryState();
  CodeContext &cc = codegen.GetCodeContext();

  // Function signature
  std::string func_name = CreateUniqueFunctionName(
      *this, IsParallel() ? "parallelWork" : "serialWork");
  auto visibility = FunctionDeclaration::Visibility::Internal;
  auto *ret_type = codegen.VoidType();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"queryState", query_state.GetType()->getPointerTo()},
      {"threadState", codegen.CharPtrType()}};

  for (uint32_t i = 0; i < pipeline_args_types.size(); i++) {
    args.emplace_back("arg" + std::to_string(i), pipeline_args_types[i]);
  }

  // The main function
  FunctionDeclaration declaration(cc, func_name, visibility, ret_type, args);
  FunctionBuilder func(cc, declaration);
  {
    auto *query_state = func.GetArgumentByPosition(0);
    auto *thread_state = func.GetArgumentByPosition(1);

    // If the pipeline is parallel, we need to call the generated init function
    if (IsParallel()) {
      thread_state = codegen->CreatePointerCast(
          thread_state, pipeline_context.GetThreadStateType()->getPointerTo());

      auto *init_func = pipeline_context.thread_init_func_;
      codegen.CallFunc(init_func, {query_state, thread_state});
    }

    // Setup the thread state access for the pipeline context
    PipelineContext::ScopedStateAccess state_access(pipeline_context,
                                                    thread_state);

    // First initialize the execution consumer
    auto &execution_consumer = compilation_ctx_.GetExecutionConsumer();
    execution_consumer.InitializePipelineState(pipeline_context);

    // Pull out the input parameters
    std::vector<llvm::Value *> pipeline_args;
    for (uint32_t i = 0; i < pipeline_args_types.size(); i++) {
      pipeline_args.push_back(func.GetArgumentByPosition(i + 2));
    }

    // Generate pipeline body
    ConsumerContext ctx(compilation_ctx_, *this, &pipeline_context);
    body(ctx, pipeline_args);

    // Finish
    func.ReturnAndFinish();
  }
  pipeline_context.pipeline_func_ = func.GetFunction();

  // The pipeline function we generated above encapsulates the logic for all
  // operators in the pipeline. If we're executing it serially then we directly
  // invoke the function now. If the pipeline is run in parallel then a dispatch
  // function must have been provided. Either way, we need to setup the call
  // now.
  //
  // In both cases, the pipeline function expects QueryState and ThreadState
  // pointers are the first two arguments. When run serially, we pass in a NULL
  // thread state pointer. When running in parallel (through a dispatch
  // function), we need to convert the QueryState type to a void * because it is
  // a runtime generated type (i.e., pre-compiled code doesn't know the layout
  // since it's dynamic)
  //
  // After this, the next arguments are whatever the caller provided to use.
  //
  // Finally, if the pipeline is run through a dispatcher function, the last
  // argument is a function pointer to the pipeline function we generated.

  std::vector<llvm::Value *> invoke_args = {codegen.GetState()};
  if (IsParallel()) {
    auto &consumer = compilation_ctx_.GetExecutionConsumer();
    invoke_args.push_back(consumer.GetThreadStatesPtr(compilation_ctx_));
  } else {
    invoke_args.push_back(codegen.NullPtr(codegen.CharPtrType()));
  }

  invoke_args.insert(invoke_args.end(), dispatch_args.begin(),
                     dispatch_args.end());

  if (dispatch_func != nullptr) {
    // Convert QueryState to void *
    invoke_args[0] =
        codegen->CreateBitOrPointerCast(invoke_args[0], codegen.VoidPtrType());
    // Tag on the pipeline function
    invoke_args.push_back(
        codegen->CreateBitCast(func.GetFunction(), codegen.VoidPtrType()));
    codegen.CallFunc(dispatch_func, invoke_args);
  } else {
    codegen.CallFunc(func.GetFunction(), invoke_args);
  }
}

////////////////////////////////////////////////////////////////////////////////
///
/// Utils
///
////////////////////////////////////////////////////////////////////////////////

// Get the stringified version of this pipeline
std::string Pipeline::GetInfo() const {
  std::string result;
  for (int32_t pi = static_cast<int32_t>(pipeline_.size()) - 1,
               sbi = static_cast<int32_t>(stage_boundaries_.size()) - 1;
       pi >= 0; pi--) {
    // Determine the plan type and append to the result
    const planner::AbstractPlan &plan = pipeline_[pi]->GetPlan();
    const std::string plan_type = PlanNodeTypeToString(plan.GetPlanNodeType());
    result.append(StringUtil::Lower(plan_type));

    // Are we at a stage boundary?
    if (sbi >= 0 && stage_boundaries_[sbi] == static_cast<uint32_t>(pi)) {
      result.append(" -//-> ");
      sbi--;
    } else if (pi > 0) {
      result.append(" -> ");
    }
  }
  return result;
}

}  // namespace codegen
}  // namespace peloton