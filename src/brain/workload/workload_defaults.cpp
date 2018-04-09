//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// workload_defaults.cpp
//
// Identification: src/brain/workload/workload_defaults.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/workload_defaults.h"

namespace peloton {
namespace brain {

const int WorkloadDefaults::NFEATS = 3;
const int WorkloadDefaults::NENCODED = 20;
const int WorkloadDefaults::NHID = 20;
const int WorkloadDefaults::NLAYERS = 2;
const float WorkloadDefaults::LR = 0.001f;
const float WorkloadDefaults::DROPOUT_RATE = 0.5f;
const float WorkloadDefaults::CLIP_NORM = 0.5f;
const int WorkloadDefaults::BATCH_SIZE = 12;
const int WorkloadDefaults::HORIZON = 216;
const int WorkloadDefaults::SEGMENT = 72;
const int WorkloadDefaults::INTERVAL = 20;
const int WorkloadDefaults::PADDLING_DAYS = 7;
const int WorkloadDefaults::BPTT = 90;
}
}