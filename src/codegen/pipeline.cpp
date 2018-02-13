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
#include "codegen/proxy/executor_context_proxy.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// PipelineContext
///
////////////////////////////////////////////////////////////////////////////////

PipelineContext::PipelineContext(Pipeline &pipeline)
    : pipeline_(pipeline),
      thread_state_type_(nullptr),
      thread_init_func_(nullptr),
      pipeline_func_(nullptr) {
  // Make room for the bool flag indicating validity
  CodeGen &codegen = pipeline.GetCompilationContext().GetCodeGen();
  state_components_.emplace_back("initialized", codegen.BoolType());
}

PipelineContext::Id PipelineContext::RegisterThreadState(std::string name,
                                                         llvm::Type *type) {
  PL_ASSERT(thread_state_type_ == nullptr);
  if (thread_state_type_ != nullptr) {
    throw Exception{"Cannot register thread state after finalization"};
  }

  PL_ASSERT(state_components_.size() < std::numeric_limits<uint8_t>::max());
  auto slot = static_cast<uint8_t>(state_components_.size());
  state_components_.emplace_back(name, type);
  return slot;
}

void PipelineContext::FinalizeThreadState(CodeGen &codegen) {
  // Quit early if we've already finalized the type
  if (thread_state_type_ != nullptr) {
    return;
  }

  // Pull out types
  std::vector<llvm::Type *> types;
  for (const auto &slot_info : state_components_) {
    types.push_back(slot_info.second);
  }

  // Build
  thread_state_type_ =
      llvm::StructType::create(codegen.GetContext(), types, "ThreadState");
}

llvm::Value *PipelineContext::AccessThreadState(CodeGen &codegen) const {
  auto *func = codegen.GetCurrentFunction();
  PL_ASSERT(func != nullptr);
  return func->GetArgumentByPosition(1);
}

llvm::Value *PipelineContext::LoadFlag(CodeGen &codegen) const {
  return LoadState(codegen, kFlagOffset);
}

void PipelineContext::StoreFlag(CodeGen &codegen, llvm::Value *flag) const {
  auto *flag_ptr = LoadStatePtr(codegen, kFlagOffset);
  codegen->CreateStore(flag, flag_ptr);
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

bool PipelineContext::IsParallel() const { return pipeline_.IsParallel(); }

Pipeline &PipelineContext::GetPipeline() { return pipeline_; }

////////////////////////////////////////////////////////////////////////////////
///
/// Pipeline
///
////////////////////////////////////////////////////////////////////////////////

// Constructor
Pipeline::Pipeline(CompilationContext &compilation_ctx)
    : compilation_ctx_(compilation_ctx), pipeline_index_(0) {
  compilation_ctx.RegisterPipeline(*this);
}

// Constructor
Pipeline::Pipeline(OperatorTranslator *translator)
    : Pipeline(translator->GetCompilationContext()) {
  Add(translator);
}

CompilationContext &Pipeline::GetCompilationContext() {
  return compilation_ctx_;
}

// Add this translator in this pipeline
void Pipeline::Add(OperatorTranslator *translator) {
  pipeline_.push_back(translator);
  pipeline_index_ = static_cast<uint32_t>(pipeline_.size()) - 1;
}

// Get the child of the current operator in this pipeline
const OperatorTranslator *Pipeline::GetChild() const {
  return pipeline_index_ < pipeline_.size() - 1 ? pipeline_[pipeline_index_ + 1]
                                                : nullptr;
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
  PL_ASSERT(pipeline_[pipeline_index_] == translator);
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

std::string ConstructFunctionName(Pipeline &pipeline,
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
  return StringUtil::Join(parts, "_");
}

void Pipeline::InitializePipeline(PipelineContext &pipeline_context) {
  if (!pipeline_context.IsParallel()) {
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->InitializePipelineState(pipeline_context);
    }
    // That's it
    return;
  }

  // Let each operator in the pipeline declare state it needs
  for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend(); riter != rend;
       ++riter) {
    (*riter)->RegisterPipelineState(pipeline_context);
  }

  // If we're the last pipeline, let the consumer know
  ExecutionConsumer &consumer = compilation_ctx_.GetExecutionConsumer();
  if (GetCompilationContext().IsLastPipeline(*this)) {
    consumer.RegisterPipelineState(pipeline_context);
  }

  // Finalize thread state type
  CodeGen &codegen = compilation_ctx_.GetCodeGen();
  pipeline_context.FinalizeThreadState(codegen);
  auto thread_state_size = static_cast<uint32_t>(
      codegen.SizeOf(pipeline_context.GetThreadStateType()));

  // Setup thread states
  llvm::Value *thread_states = consumer.GetThreadStatesPtr(compilation_ctx_);
  codegen.Call(ThreadStatesProxy::Reset,
               {thread_states, codegen.Const32(thread_state_size)});

  // Generate an initialization function
  RuntimeState &runtime_state = compilation_ctx_.GetRuntimeState();
  CodeContext &cc = codegen.GetCodeContext();

  auto func_name = ConstructFunctionName(*this, "initializeWorkerState");
  auto visibility = FunctionDeclaration::Visibility::Internal;
  auto *ret_type = codegen.VoidType();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.GetType()->getPointerTo()},
      {"threadState", pipeline_context.GetThreadStateType()->getPointerTo()}};

  FunctionDeclaration init_decl{cc, func_name, visibility, ret_type, args};
  FunctionBuilder init_func{cc, init_decl};
  {
    // Let each translator initialize
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->InitializePipelineState(pipeline_context);
    }
    // That's it
    init_func.ReturnAndFinish();
  }
  pipeline_context.thread_init_func_ = init_func.GetFunction();
}

void Pipeline::CompletePipeline(PipelineContext &pipeline_context) {
  if (!pipeline_context.IsParallel()) {
    // Let operators in the pipeline do some post-pipeline work
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->FinishPipeline(pipeline_context);
    }
    // Let operators in the pipeline clean up any pipeline state
    for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend();
         riter != rend; ++riter) {
      (*riter)->TearDownPipelineState(pipeline_context);
    }
    // That's it
    return;
  }
}

void Pipeline::RunSerial(const std::function<void(ConsumerContext &)> &body) {
  Run(nullptr, {}, {},
      [&body](ConsumerContext &ctx,
              UNUSED_ATTRIBUTE const std::vector<llvm::Value *> &args) {
        body(ctx);
      });
}

void Pipeline::RunParallel(
    llvm::Function *launch_func, const std::vector<llvm::Value *> &launch_args,
    const std::vector<llvm::Type *> &pipeline_args_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  PL_ASSERT(IsParallel() && "Cannot run parallel if pipeline isn't parallel");
  Run(launch_func, launch_args, pipeline_args_types, body);
}

void Pipeline::Run(
    llvm::Function *launch_func, const std::vector<llvm::Value *> &launch_args,
    const std::vector<llvm::Type *> &pipeline_arg_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  // Create context
  PipelineContext pipeline_context{*this};

  // Initialize the pipeline
  InitializePipeline(pipeline_context);

  // Generate pipeline
  DoRun(pipeline_context, launch_func, launch_args, pipeline_arg_types, body);

  // Finish
  CompletePipeline(pipeline_context);
}

void Pipeline::DoRun(
    PipelineContext &pipeline_context, llvm::Function *launch_func,
    const std::vector<llvm::Value *> &launch_args,
    const std::vector<llvm::Type *> &pipeline_args_types,
    const std::function<void(ConsumerContext &,
                             const std::vector<llvm::Value *> &)> &body) {
  CodeGen &codegen = compilation_ctx_.GetCodeGen();
  RuntimeState &runtime_state = compilation_ctx_.GetRuntimeState();
  CodeContext &cc = codegen.GetCodeContext();

  // Function signature
  std::string func_name = ConstructFunctionName(
      *this, IsParallel() ? "parallelWork" : "serialWork");
  auto visibility = FunctionDeclaration::Visibility::Internal;
  auto *ret_type = codegen.VoidType();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.GetType()->getPointerTo()}};

  if (IsParallel()) {
    args.emplace_back("threadState",
                      pipeline_context.GetThreadStateType()->getPointerTo());
    for (uint32_t i = 0; i < pipeline_args_types.size(); i++) {
      args.emplace_back("arg" + std::to_string(i), pipeline_args_types[i]);
    }
  }

  // The main function
  FunctionDeclaration decl{cc, func_name, visibility, ret_type, args};
  FunctionBuilder func{cc, decl};
  {
    if (IsParallel()) {
      auto *query_state = func.GetArgumentByPosition(0);
      auto *thread_state = func.GetArgumentByPosition(1);
      codegen.CallFunc(pipeline_context.thread_init_func_,
                       {query_state, thread_state});
    }

    // First initialize the execution consumer
    auto &execution_consumer = compilation_ctx_.GetExecutionConsumer();
    execution_consumer.InitializePipelineState(pipeline_context);

    // Pull out the input parameters
    std::vector<llvm::Value *> pipeline_args;
    if (IsParallel()) {
      for (uint32_t i = 0; i < pipeline_args_types.size(); i++) {
        pipeline_args.push_back(func.GetArgumentByPosition(i + 2));
      }
    }

    // Generate pipeline body
    ConsumerContext ctx{GetCompilationContext(), *this, &pipeline_context};
    body(ctx, pipeline_args);

    // Finish
    func.ReturnAndFinish();
  }
  pipeline_context.pipeline_func_ = func.GetFunction();

  // If a launching function is provided, invoke it passing the pipeline
  // function we just constructed as the last argument
  if (launch_func != nullptr) {
    // Construct arguments to launch function
    llvm::Value *query_state =
        codegen->CreateBitCast(codegen.GetState(), codegen.VoidPtrType());

    ExecutionConsumer &consumer = compilation_ctx_.GetExecutionConsumer();
    llvm::Value *thread_states = consumer.GetThreadStatesPtr(compilation_ctx_);

    std::vector<llvm::Value *> new_launch_args = {query_state, thread_states};
    new_launch_args.insert(new_launch_args.end(), launch_args.begin(),
                           launch_args.end());
    new_launch_args.emplace_back(
        codegen->CreateBitCast(func.GetFunction(), codegen.VoidPtrType()));
    //    new_launch_args.emplace_back(pipeline_func.GetFunction());

    // Call the launch function
    codegen.CallFunc(launch_func, new_launch_args);
  } else {
    // Directly launch the pipeline function
    codegen.CallFunc(func.GetFunction(), {codegen.GetState()});
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