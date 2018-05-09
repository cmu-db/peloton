//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// eigen_util.h
//
// Identification: src/include/brain/util/eigen_util.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>
#include "common/internal_types.h"



namespace peloton{
namespace brain{

class EigenUtil {
 public:
  static matrix_eig MatrixTToEigenMat(matrix_t &mat);
  static matrix_t EigenMatToMatrixT(matrix_eig &mat);
  static std::vector<float> FlattenMatrix(const matrix_eig &mat);
};

}
}
