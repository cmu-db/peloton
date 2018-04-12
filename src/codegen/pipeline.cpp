//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pipeline.cpp
//
// Identification: src/codegen/pipeline.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/pipeline.h"

#include "codegen/operator/operator_translator.h"

namespace peloton {
namespace codegen {

// Constructor
Pipeline::Pipeline() : pipeline_index_(0) {}

// Constructor
Pipeline::Pipeline(const OperatorTranslator *translator) { Add(translator); }

// Add this translator in this pipeline
void Pipeline::Add(const OperatorTranslator *translator) {
  pipeline_.push_back(translator);
  pipeline_index_ = static_cast<uint32_t>(pipeline_.size()) - 1;
}

// Install a stage boundary at the input into the given translator
void Pipeline::InstallBoundaryAtInput(const OperatorTranslator *translator) {
  // Validate the assumption
  (void)translator;
  PELOTON_ASSERT(pipeline_[pipeline_index_] == translator);
  stage_boundaries_.push_back(pipeline_index_ + 1);
}

// Install a stage boundary at the input into the given translator
void Pipeline::InstallBoundaryAtOutput(const OperatorTranslator *translator) {
  // Validate the assumption
  (void)translator;
  PELOTON_ASSERT(pipeline_[pipeline_index_] == translator);
  if (!stage_boundaries_.empty() &&
      stage_boundaries_.back() != pipeline_index_) {
    stage_boundaries_.push_back(pipeline_index_);
  }
}

bool Pipeline::AtStageBoundary() const {
  return std::find(stage_boundaries_.begin(), stage_boundaries_.end(),
                   pipeline_index_) != stage_boundaries_.end();
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

// Get the stringified version of this pipeline
std::string Pipeline::GetInfo() const {
  std::string result;
  for (int32_t pi = static_cast<int32_t>(pipeline_.size()) - 1,
               sbi = static_cast<int32_t>(stage_boundaries_.size()) - 1;
       pi >= 0; pi--) {
    result.append(pipeline_[pi]->GetName());
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