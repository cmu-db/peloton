#pragma once

#include <algorithm>
#include <vector>
// Impose Row-major to avoid confusion
#define EIGEN_DEFAULT_TO_ROW_MAJOR
#include "Eigen/Dense"

// TODO(saatvik): Generalize Eigen utilities across all types
typedef std::vector<std::vector<float>> matrix_t;
typedef Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>
    matrix_eig;


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
