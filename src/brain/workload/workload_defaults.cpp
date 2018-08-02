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

const int CommonWorkloadDefaults::HORIZON = 216;
const int CommonWorkloadDefaults::INTERVAL = 100;
const int CommonWorkloadDefaults::PADDLING_DAYS = 7;
const int CommonWorkloadDefaults::ESTOP_PATIENCE = 10;
const float CommonWorkloadDefaults::ESTOP_DELTA = 0.01f;

const int LSTMWorkloadDefaults::NFEATS = 3;
const int LSTMWorkloadDefaults::NENCODED = 20;
const int LSTMWorkloadDefaults::NHID = 20;
const int LSTMWorkloadDefaults::NLAYERS = 2;
const float LSTMWorkloadDefaults::LR = 0.01f;
const float LSTMWorkloadDefaults::DROPOUT_RATE = 0.5f;
const float LSTMWorkloadDefaults::CLIP_NORM = 0.5f;
const int LSTMWorkloadDefaults::BATCH_SIZE = 12;
const int LSTMWorkloadDefaults::BPTT = 90;
const int LSTMWorkloadDefaults::EPOCHS = 100;

const int LinearRegWorkloadDefaults::BPTT = 90;

const int KernelRegWorkloadDefaults::BPTT = 90;

}  // namespace brain
}  // namespace peloton