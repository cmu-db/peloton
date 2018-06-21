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

/**
 * Common defaults(that should be uniform) across all models
 * for the Workload Forecasting task
 */
const int CommonWorkloadDefaults::HORIZON = 216;
const int CommonWorkloadDefaults::SEGMENT = 72;
// TODO(saatviks): Currently assuming and using a default interval/aggregate
// (term used in QB5000 code) of 1. Need to adjust this accordingly in many
// places.
const int CommonWorkloadDefaults::INTERVAL = 1;
// TODO(saatviks): Need to add paddling days term.
const int CommonWorkloadDefaults::PADDLING_DAYS = 7;

/**
 * LSTM Model defaults for Workload Forecasting task
 */
// Default unit = minutes?
const int LSTMWorkloadDefaults::NFEATS = 3;
const int LSTMWorkloadDefaults::NENCODED = 20;
const int LSTMWorkloadDefaults::NHID = 20;
const int LSTMWorkloadDefaults::NLAYERS = 2;
const float LSTMWorkloadDefaults::LR = 0.001f;
const float LSTMWorkloadDefaults::DROPOUT_RATE = 0.5f;
const float LSTMWorkloadDefaults::CLIP_NORM = 0.5f;
const int LSTMWorkloadDefaults::BATCH_SIZE = 12;
const int LSTMWorkloadDefaults::BPTT = 90;

/**
 * LinearReg Model defaults for Workload Forecasting task
 */
const int LinearRegWorkloadDefaults::REGRESSION_DIM = 72;

/**
 * KernelReg Model defaults for Workload Forecasting task
 */
const int KernelRegWorkloadDefaults::REGRESSION_DIM = 72;

}  // namespace brain
}