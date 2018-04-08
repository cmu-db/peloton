//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_util.h
//
// Identification: src/include/brain/util/tf_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tensorflow/c/c_api.h>

/**
 * Simple utility functions associated with Tensorflow
 */

namespace peloton {
namespace brain {
class TFUtil {
 public:
  static const char *GetTFVersion() {
    return TF_Version();
  }
};
}
}
