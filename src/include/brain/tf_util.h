//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_util.h
//
// Identification: src/include/brain/tf_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <tensorflow/c/c_api.h>

namespace peloton {
namespace brain {
class TFUtil {
 public:
  static const char *GetTFVersion() {
    return TF_Version();
  }
  static TF_Graph* GetNewGraph() {
    auto graph = TF_NewGraph();
    return graph;
  }
};
}
}
