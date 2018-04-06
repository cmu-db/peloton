//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// internal_types.h
//
// Identification: src/include/brain/util/internal_types.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

// Impose Row-major to avoid confusion
#define EIGEN_DEFAULT_TO_ROW_MAJOR
#include "eigen3/Eigen/Dense"

// TODO(saatvik): Generalize Eigen utilities across all types
typedef std::vector<std::vector<float>> matrix_t;
typedef Eigen::Matrix<float, Eigen::Dynamic, Eigen::Dynamic, Eigen::RowMajor>
    matrix_eig;