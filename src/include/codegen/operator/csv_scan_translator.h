//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// csv_scan_translator.h
//
// Identification: src/include/codegen/operator/csv_scan_translator.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/operator/operator_translator.h"

namespace peloton {

namespace planner {
class CSVScanPlan;
}  // namespace planner

namespace codegen {
class CompilationContext;
class Pipeline;
}  // namespace codegen

namespace codegen {

//===----------------------------------------------------------------------===//
// A translator for CSV file scans
//===----------------------------------------------------------------------===//
class CSVScanTranslator : public OperatorTranslator {
 public:
  // Constructor
  CSVScanTranslator(const planner::CSVScanPlan &scan,
                    CompilationContext &context, Pipeline &pipeline);

  void InitializeState() override;

  void DefineAuxiliaryFunctions() override;

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeState(), file scans don't have any state
  void TearDownState() override;

  // Get a stringified version of this translator
  std::string GetName() const override;

 private:
  // Plan accessor
  const planner::CSVScanPlan &GetScanPlan() const { return scan_; }

  llvm::Value *ConstructColumnDescriptor() const;

 private:
  // The scan
  const planner::CSVScanPlan &scan_;

  // The scanner state ID
  RuntimeState::StateID scanner_id_;

  // The generated CSV scan consumer function
  llvm::Function *consumer_func_;
};

}  // namespace codegen
}  // namespace peloton