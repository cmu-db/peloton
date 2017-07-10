//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// pipeline.h
//
// Identification: src/include/codegen/pipeline.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>
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
  // Constructor
  Pipeline();
  Pipeline(const OperatorTranslator *translator);

  // Add the provided translator to this pipeline
  void Add(const OperatorTranslator *translator);

  void InstallBoundaryAtInput(const OperatorTranslator *translator);
  void InstallBoundaryAtOutput(const OperatorTranslator *translator);

  bool AtStageBoundary() const;

  // Get the child of the current operator in this pipeline
  const OperatorTranslator *GetChild() const;

  // Move to the next step in this pipeline
  const OperatorTranslator *NextStep();

  uint32_t GetNumStages() const;
  uint32_t GetTranslatorStage(const OperatorTranslator *translator) const;

  // Get a stringified version of this pipeline
  std::string GetInfo() const;

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