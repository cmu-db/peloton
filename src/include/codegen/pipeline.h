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

namespace peloton {
namespace codegen {

class OperatorTranslator;
class CompilationContext;
class ConsumerContext;

//===----------------------------------------------------------------------===//
// A pipeline represents operators in a query plan that can be fully pipelined.
// Peloton pipelines are decomposed further into stages. Operators in a
// stage are fully pipelined/fused together, while whole stages communicate
// through cache-resident vectors of TIDs.
//===----------------------------------------------------------------------===//
class Pipeline {
 public:
  /// Enum indicitating level of parallelism
  enum Parallelism { Serial = 0, Parallel = 1 };

  /// Constructor
  Pipeline();
  Pipeline(const OperatorTranslator *translator);

  /// Add the provided translator to this pipeline
  void Add(const OperatorTranslator *translator);

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

  /// Return the total number of stages in the pipeline
  uint32_t GetNumStages() const;

  /// Get the stage the given translator is in
  uint32_t GetTranslatorStage(const OperatorTranslator *translator) const;

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Serial/Parallel execution
  ///
  //////////////////////////////////////////////////////////////////////////////

  void RunSerial(const std::function<void()> &func);
  void RunParallel(const std::function<void()> &func);

  //////////////////////////////////////////////////////////////////////////////
  ///
  /// Utilities
  ///
  //////////////////////////////////////////////////////////////////////////////

  /// Get a stringified version of this pipeline
  std::string GetInfo() const;

 private:
  std::string GetSimplePipelineName() const;

 private:
  // The pipeline of operators, progress is made from the end to the beginning
  std::vector<const OperatorTranslator *> pipeline_;

  // The index into the pipeline that points to the current working operator
  uint32_t pipeline_index_;

  // Points in the pipeline that represents boundaries between stages.
  // A value, i, in this list means there is a stage boundary between operators
  // i-1 and i in the pipeline.
  std::vector<uint32_t> stage_boundaries_;
};

}  // namespace codegen
}  // namespace peloton