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

/// Forward declare Pipeline that's declared at the end of this file
class Pipeline;

class PipelineContext {
  friend class Pipeline;

  static const uint32_t kFlagOffset = 0;

 public:
  using Id = uint32_t;

  /// Constructor
  explicit PipelineContext(Pipeline &pipeline);

  /// Register some state
  Id RegisterState(std::string name, llvm::Type *type);

  /// Build the final type of the thread state. Once finalized, the thread
  /// state type is immutable.
  void FinalizeState(CodeGen &codegen);
  llvm::Type *GetThreadStateType() const { return thread_state_type_; }

  /// State access
  llvm::Value *AccessThreadState(CodeGen &codegen) const;
  llvm::Value *LoadFlag(CodeGen &codegen) const;
  void StoreFlag(CodeGen &codegen, llvm::Value *flag) const;
  void MarkInitialized(CodeGen &codegen) const;
  llvm::Value *LoadStatePtr(CodeGen &codegen, Id) const;
  llvm::Value *LoadState(CodeGen &codegen, Id state_id) const;
  uint32_t GetEntryOffset(CodeGen &codegen, Id state_id) const;

  /// Is the pipeline associated with this context parallel?
  bool IsParallel() const;

  /// Return the pipeline this context is associated with
  Pipeline &GetPipeline();

  class ScopedStateAccess {
   public:
    ScopedStateAccess(PipelineContext &pipeline_ctx, llvm::Value *thread_state)
        : pipeline_ctx_(pipeline_ctx),
          prev_thread_state_(pipeline_ctx.thread_state_) {
      pipeline_ctx.thread_state_ = thread_state;
    }
    ~ScopedStateAccess() { pipeline_ctx_.thread_state_ = prev_thread_state_; }

   private:
    PipelineContext &pipeline_ctx_;
    llvm::Value *prev_thread_state_;
  };

  /**
   * A handy class to loop over all thread states
   */
  class LoopOverStates {
   public:
    LoopOverStates(PipelineContext &pipeline_ctx);
    ~LoopOverStates();
  };

 private:
  // The pipeline
  Pipeline &pipeline_;
  // The elements of the thread state for this pipeline
  std::vector<std::pair<std::string, llvm::Type *>> state_components_;
  llvm::Type *thread_state_type_;
  llvm::Value *thread_state_;
  // The generate thread initialization function and pipeline function
  llvm::Function *thread_init_func_;
  llvm::Function *pipeline_func_;
};

/**
 * A pipeline represents an ordered sequence of relational operators that
 * operate on tuple data without explicit copying or materialization. Tuples are
 * read at the start of the pipeline, are passed through each operator, and are
 * materialized in some form only at the end of the pipeline.
 *
 * Peloton pipelines are decomposed further into stages. Stages represent
 * sub-sequences of pipeline operators. Peloton reads batches of tuples at the
 * start of the pipeline, and passes this batch through each stage. This enables
 * a hybrid tuple-at-time and vectorized processing model. Each tuple batch is
 * accompanied by a cache-resident selection vector (also known as a position
 * list) to determine the validity of each tuple in the batch. Refer to
 * @refitem peloton::codegen::RowBatch for more details.
 *
 * Pipelines form the unit of parallelism in Peloton. Each pipeline can either
 * be launched serially or in parallel.
 */
class Pipeline {
 public:
  /**
   * Enum class representing a degree of parallelism. The Serial and Parallel
   * values are clear and explicit. The Flexible option should be used when both
   * serial and parallel operation is supported, but no preference is taken.
   */
  enum class Parallelism : uint32_t { Serial = 0, Flexible = 1, Parallel = 2 };

  explicit Pipeline(CompilationContext &compilation_ctx);

  Pipeline(OperatorTranslator *translator, Parallelism parallelism);

  void Add(OperatorTranslator *translator, Parallelism parallelism);

  void MarkSource(OperatorTranslator *translator, Parallelism parallelism);

  const OperatorTranslator *NextStep();

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Stages
  ///
  //////////////////////////////////////////////////////////////////////////////

  void InstallStageBoundary(const OperatorTranslator *translator);

  bool AtStageBoundary() const;

  uint32_t GetTranslatorStage(const OperatorTranslator *translator) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Serial/Parallel execution
  ///
  //////////////////////////////////////////////////////////////////////////////

  bool IsParallel() const { return parallelism_ == Parallelism::Parallel; }

  void RunSerial(const std::function<void(ConsumerContext &)> &body);

  void RunParallel(
      llvm::Function *dispatch_func,
      const std::vector<llvm::Value *> &dispatch_args,
      const std::vector<llvm::Type *> &pipeline_args_types,
      const std::function<void(ConsumerContext &,
                               const std::vector<llvm::Value *> &)> &body);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Return the unique ID of this pipeline
  uint32_t GetId() const { return id_; }

  /// Pipeline equality check (this is more of an **identity** equality check)
  bool operator==(const Pipeline &other) const { return id_ == other.id_; }
  bool operator!=(const Pipeline &other) const { return !(*this == other); }

  /// Compilation context accessor
  CompilationContext &GetCompilationContext() { return compilation_ctx_; }

  /// Get a stringified version of this pipeline
  std::string GetInfo() const;

  /// Build a simple name for this pipeline
  std::string ConstructPipelineName() const;

 private:
  /// Initialize the state for this pipeline
  void InitializePipeline(PipelineContext &pipeline_context);
  void CompletePipeline(PipelineContext &pipeline_context);
  void Run(llvm::Function *dispatch_func,
           const std::vector<llvm::Value *> &dispatch_args,
           const std::vector<llvm::Type *> &pipeline_arg_types,
           const std::function<void(ConsumerContext &,
                                    const std::vector<llvm::Value *> &)> &body);
  void DoRun(PipelineContext &pipeline_context, llvm::Function *dispatch_func,
             const std::vector<llvm::Value *> &dispatch_args,
             const std::vector<llvm::Type *> &pipeline_args_types,
             const std::function<void(
                 ConsumerContext &, const std::vector<llvm::Value *> &)> &body);

  /// Return the total number of stages in the pipeline
  uint32_t GetNumStages() const;

 private:
  // Unique ID of this pipeline
  uint32_t id_;

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

  // Level of parallelism
  Parallelism parallelism_;
};

}  // namespace codegen
}  // namespace peloton