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

struct LSTMWorkloadDefaults {
  static const int NFEATS;
  static const int NENCODED;
  static const int NHID;
  static const int NLAYERS;
  static const float LR;
  static const float DROPOUT_RATE;
  static const float CLIP_NORM;
  static const int BATCH_SIZE;
  static const int HORIZON;
  static const int SEGMENT;
  static const int INTERVAL;
  static const int PADDLING_DAYS;
  static const int BPTT;
};
}  // namespace brain
}  // namespace peloton