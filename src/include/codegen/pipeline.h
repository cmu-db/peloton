//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pipeline.h
//
// Identification: src/include/codegen/pipeline.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace llvm {
class Function;
class Type;
class Value;
}  // namespace llvm

namespace peloton {
namespace codegen {

class CodeGen;
class CompilationContext;
class ConsumerContext;
class OperatorTranslator;

class Pipeline;

class PipelineContext {
  friend class Pipeline;

 public:
  using SlotId = uint8_t;

  /// Constructor
  explicit PipelineContext(Pipeline &pipeline);

  /// Register some state
  SlotId RegisterThreadState(std::string name, llvm::Type *type);

  /// Build the final type of the thread state. Once finalized, the thread
  /// state type is immutable.
  void FinalizeThreadState(CodeGen &codegen);
  llvm::Type *GetThreadStateType() const { return thread_state_; }

  /// Is the pipeline associated with this context parallel?
  bool IsParallel() const;

  /// Return the pipeline this context is associated with
  Pipeline &GetPipeline();

 private:
  // The pipeline
  Pipeline &pipeline_;
  // The elements of the thread state for this pipeline
  std::vector<std::pair<std::string, llvm::Type *>> state_components_;
  llvm::Type *thread_state_;
  // The generate thread initialization function and pipeline function
  llvm::Function *thread_init_func_;
  llvm::Function *pipeline_func_;
};

//===----------------------------------------------------------------------===//
//
// A pipeline represents operators in a query plan that can be fully pipelined.
// Peloton pipelines are decomposed further into stages. Operators in a
// stage are fully pipelined/fused together, while whole stages communicate
// through cache-resident vectors of TIDs.
//
//===----------------------------------------------------------------------===//
class Pipeline {
 public:
  /// Enum indicating level of parallelism
  enum Parallelism { Serial = 0, Parallel = 1 };

  /// Constructor
  Pipeline(CompilationContext &compilation_ctx);
  Pipeline(OperatorTranslator *translator);

  /// Compilation context accessor
  CompilationContext &GetCompilationContext();

  /// Add the provided translator to this pipeline
  void Add(OperatorTranslator *translator);

  /// Get the child of the current operator in this pipeline
  const OperatorTranslator *GetChild() const;

  /// Move to the next step in this pipeline
  const OperatorTranslator *NextStep();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Stages
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Install a stage boundary in front of the given operator
  void InstallStageBoundary(const OperatorTranslator *translator);

  /// Is the pipeline position currently sitting at a stage boundary?
  bool AtStageBoundary() const;

  /// Get the stage the given translator is in
  uint32_t GetTranslatorStage(const OperatorTranslator *translator) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Serial/Parallel execution
  ///
  //////////////////////////////////////////////////////////////////////////////

  bool IsParallel() const { return false; }
  void RunSerial(const std::function<void(ConsumerContext &)> &body);
  void RunParallel(
      llvm::Function *launch_func,
      const std::vector<llvm::Value *> &launch_args,
      const std::vector<llvm::Type *> &pipeline_args_types,
      const std::function<void(ConsumerContext &,
                               const std::vector<llvm::Value *> &)> &body);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Get a stringified version of this pipeline
  std::string GetInfo() const;

  /// Build a simple name for this pipeline
  std::string ConstructPipelineName() const;

 private:
  /// Initialize the state for this pipeline
  void InitializePipeline(PipelineContext &pipeline_context);
  void CompletePipeline(PipelineContext &pipeline_context);
  void Run(llvm::Function *launch_func,
           const std::vector<llvm::Value *> &launch_args,
           const std::vector<llvm::Type *> &pipeline_arg_types,
           const std::function<void(ConsumerContext &,
                                    const std::vector<llvm::Value *> &)> &body);
  void DoRun(PipelineContext &pipeline_context, llvm::Function *launch_func,
             const std::vector<llvm::Value *> &launch_args,
             const std::vector<llvm::Type *> &pipeline_args_types,
             const std::function<void(
                 ConsumerContext &, const std::vector<llvm::Value *> &)> &body);

  /// Return the total number of stages in the pipeline
  uint32_t GetNumStages() const;

 private:
  // The compilation context
  CompilationContext &compilation_ctx_;

  // The pipeline of operators, progress is made from the end to the beginning
  std::vector<OperatorTranslator *> pipeline_;

  // The index into the pipeline that points to the current working operator
  uint32_t pipeline_index_;

  // Points in the pipeline that represents boundaries between stages.
  // A value, i, in this list means there is a stage boundary between operators
  // i-1 and i in the pipeline.
  std::vector<uint32_t> stage_boundaries_;
};

}  // namespace codegen
}  // namespace peloton