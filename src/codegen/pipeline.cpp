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
#include "codegen/function_builder.h"
#include "codegen/operator/operator_translator.h"
#include "planner/abstract_plan.h"
#include "util/string_util.h"

namespace peloton {
namespace codegen {

////////////////////////////////////////////////////////////////////////////////
///
/// PipelineContext
///
////////////////////////////////////////////////////////////////////////////////

PipelineContext::PipelineContext(Pipeline &pipeline) : pipeline_(pipeline) {}

Pipeline &PipelineContext::GetPipeline() { return pipeline_; }

CompilationContext &PipelineContext::GetCompilationContext() {
  return pipeline_.GetCompilationContext();
}

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
  auto pipeline_pos = compilation_ctx.GetPipelinePosition(pipeline);
  auto pipeline_name = pipeline.ConstructPipelineName();
  return StringUtil::Format("_%" PRId64 "_pipeline_%u_%s_%s", cc.GetID(),
                            pipeline_pos, prefix.c_str(),
                            pipeline_name.c_str());
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
  // First initialize the execution consumer, then each of the translators
  auto &execution_consumer = compilation_ctx_.GetExecutionConsumer();
  execution_consumer.InitializePipelineState(pipeline_context);

  for (auto riter = pipeline_.rbegin(), rend = pipeline_.rend(); riter != rend;
       ++riter) {
    (*riter)->InitializePipelineState(pipeline_context);
  }
}

void Pipeline::RunSerial(const std::function<void()> &body) {
  PipelineContext pipeline_context{*this};

  // Function name and arguments
  auto func_name = ConstructFunctionName(*this, "serialWork");

  RuntimeState &runtime_state = compilation_ctx_.GetRuntimeState();
  std::vector<FunctionDeclaration::ArgumentInfo> args = {
      {"runtimeState", runtime_state.GetType()->getPointerTo()}};

  CodeGen &codegen = compilation_ctx_.GetCodeGen();
  CodeContext &cc = codegen.GetCodeContext();
  FunctionBuilder pipeline_func{cc, func_name, codegen.VoidType(), args};
  {
    // First initialize the pipeline
    InitializePipeline(pipeline_context);
    // Generate body
    body();
    // Finish
    pipeline_func.ReturnAndFinish();
  }

  // Invoke the pipeline function
  codegen.CallFunc(pipeline_func.GetFunction(), {codegen.GetState()});
}

void Pipeline::RunParallel(UNUSED_ATTRIBUTE const std::function<void()> &func) {
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

    // Append plan type to result
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