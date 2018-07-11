//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// workload_defaults.h
//
// Identification: src/include/brain/workload/workload_defaults.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

/**
 * This header file contains default attributes
 * associated with the workload prediction task
 **/

namespace peloton {
namespace brain {

/**
 * Common defaults(that should be uniform) across all models
 * for the Workload Forecasting task
 * // TODO(saatviks): SEGMENT/AGGREGATE not needed?
 * // TODO(saatviks): Look into using a timer type(Default unit = minutes)
 */
struct CommonWorkloadDefaults {
  static const int HORIZON;
  static const int INTERVAL;
  static const int PADDLING_DAYS;
  // Early Stop parameters
  static const int ESTOP_PATIENCE;
  static const float ESTOP_DELTA;
};

/**
 * LSTM Model defaults for Workload Forecasting task
 */
struct LSTMWorkloadDefaults {
  static const int NFEATS;
  static const int NENCODED;
  static const int NHID;
  static const int NLAYERS;
  static const float LR;
  static const float DROPOUT_RATE;
  static const float CLIP_NORM;
  static const int BATCH_SIZE;
  static const int BPTT;
  static const int EPOCHS;
};

/**
 * LinearReg Model defaults for Workload Forecasting task
 */
struct LinearRegWorkloadDefaults {
  static const int BPTT;
};

/**
 * KernelReg Model defaults for Workload Forecasting task
 */
struct KernelRegWorkloadDefaults {
  static const int BPTT;
};
}  // namespace brain
}  // namespace peloton