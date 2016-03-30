//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hyadapt_workload.h
//
// Identification: src/backend/benchmark/hyadapt/hyadapt_workload.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/hyadapt/hyadapt_configuration.h"

namespace peloton {
namespace benchmark {
namespace hyadapt {

extern configuration state;

void CreateAndLoadTable(LayoutType layout_type);

void RunDirectTest();

void RunAggregateTest();

void RunArithmeticTest();

void RunJoinTest();

void RunSubsetTest(SubsetType subset_test_type, double fraction);

void RunProjectivityExperiment();

void RunSelectivityExperiment();

void RunOperatorExperiment();

void RunVerticalExperiment();

void RunSubsetExperiment();

void RunAdaptExperiment();

void RunWeightExperiment();

void RunReorgExperiment();

void RunDistributionExperiment();

void RunJoinExperiment();

void RunInsertExperiment();

void RunVersionExperiment();

void RunHyriseExperiment();

void RunConcurrencyExperiment();

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
