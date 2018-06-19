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

  void InitializeQueryState() override;

  void DefineAuxiliaryFunctions() override;

  // The method that produces new tuples
  void Produce() const override;

  // Scans are leaves in the query plan and, hence, do not consume tuples
  void Consume(ConsumerContext &, RowBatch &) const override {}
  void Consume(ConsumerContext &, RowBatch::Row &) const override {}

  // Similar to InitializeState(), file scans don't have any state
  void TearDownQueryState() override;

 private:
  // The set of attributes output by the csv scan
  std::vector<const planner::AttributeInfo *> output_attributes_;

  // The scanner state ID
  QueryState::Id scanner_id_;

  // The generated CSV scan consumer function
  llvm::Function *consumer_func_;
};

}  // namespace codegen
}  // namespace peloton