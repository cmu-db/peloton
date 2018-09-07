//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity_defaults.cpp
//
// Identification: src/brain/workload/selectivity_defaults.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/selectivity/selectivity_defaults.h"

namespace peloton {
namespace brain {

const int AugmentedNNDefaults::COLUMN_NUM = 1;
const int AugmentedNNDefaults::ORDER = 1;
const int AugmentedNNDefaults::NEURON_NUM = 16;
const float AugmentedNNDefaults::LR = 0.1f;
const int AugmentedNNDefaults::BATCH_SIZE = 256;
const int AugmentedNNDefaults::EPOCHS = 600;


}  // namespace brain
}  // namespace peloton
