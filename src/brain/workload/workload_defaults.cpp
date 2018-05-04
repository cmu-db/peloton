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

const int LSTMWorkloadDefaults::NFEATS = 3;
const int LSTMWorkloadDefaults::NENCODED = 20;
const int LSTMWorkloadDefaults::NHID = 20;
const int LSTMWorkloadDefaults::NLAYERS = 2;
const float LSTMWorkloadDefaults::LR = 0.001f;
const float LSTMWorkloadDefaults::DROPOUT_RATE = 0.5f;
const float LSTMWorkloadDefaults::CLIP_NORM = 0.5f;
const int LSTMWorkloadDefaults::BATCH_SIZE = 12;
const int LSTMWorkloadDefaults::HORIZON = 216;
const int LSTMWorkloadDefaults::SEGMENT = 72;
const int LSTMWorkloadDefaults::INTERVAL = 20;
const int LSTMWorkloadDefaults::PADDLING_DAYS = 7;
const int LSTMWorkloadDefaults::BPTT = 90;
}
}