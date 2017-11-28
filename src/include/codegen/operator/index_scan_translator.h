//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator.h
//
// Identification: src/include/codegen/operator/index_scan_translator.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"
#include "codegen/operator/operator_translator.h"
#include "codegen/scan_callback.h"
#include "index/index.h"

namespace peloton {

namespace planner {
class IndexScanPlan;
}  // namespace planner

namespace index {
class Index;
}  // namespace index

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for index scans
//===----------------------------------------------------------------------===//
class IndexScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  IndexScanTranslator(const planner::IndexScanPlan &index_scan,
                      CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override {}

  // Index scans don't rely on any auxiliary functions
  void DefineAuxiliaryFunctions() override {}

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeState(), index scans don't have any state
  void TearDownState() override {}

  // Get a stringified version of this translator
  std::string GetName() const override;

 private:
  // Plan accessor
  const planner::IndexScanPlan &GetIndexScanPlan() const { return index_scan_; }

  // Index accessor
  const index::Index &GetIndex() const;

 private:
  // The scan
  const planner::IndexScanPlan &index_scan_;

  // The ID of the selection vector in runtime state
  RuntimeState::StateID selection_vector_id_;

  // The code-generating table instance
  //  codegen::ArtIndex index_;
};
}
}
