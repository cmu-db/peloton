//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// selectivity_defaults.h
//
// Identification: src/include/brain/workload/selectivity_defaults.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

/**
 * This header file contains default attributes
 * associated with the selectivity prediction task
 **/

namespace peloton {
namespace brain {

struct AugmentedNNDefaults {
  static const int COLUMN_NUM;
  static const int ORDER;
  static const int NEURON_NUM;
  static const float LR;
  static const int BATCH_SIZE;
  static const int EPOCHS;
};

}  // namespace brain
}  // namespace peloton
