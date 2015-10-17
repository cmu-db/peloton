//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// workload.h
//
// Identification: benchmark/hyadapt/workload.h
//
// Copyright (c) 2015, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "backend/benchmark/hyadapt/configuration.h"

namespace peloton {
namespace benchmark {
namespace hyadapt{

enum SubsetType{
  SUBSET_TYPE_INVALID = 0,

  SUBSET_TYPE_SINGLE_GROUP = 1,
  SUBSET_TYPE_MULTIPLE_GROUP = 2

};

extern configuration state;

extern storage::DataTable *hyadapt_table;

void CreateAndLoadTable(LayoutType layout_type);

void RunDirectTest();

void RunAggregateTest();

void RunArithmeticTest();

void RunSubsetTest(SubsetType subset_test_type, double fraction);

void RunProjectivityExperiment();

void RunSelectivityExperiment();

void RunOperatorExperiment();

void RunVerticalExperiment();

void RunSubsetExperiment();

}  // namespace hyadapt
}  // namespace benchmark
}  // namespace peloton
