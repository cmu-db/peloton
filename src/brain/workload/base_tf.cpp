//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// base_tf.cpp
//
// Identification: src/brain/workload/base_tf.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/workload/base_tf.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"
#include "util/file_util.h"

namespace peloton {
namespace brain {

/**
 * Normalizer methods
 */
void Normalizer::Fit(const matrix_eig &X) {
  if (do_normalize_) {
    min_ = 1 - X.minCoeff();
    matrix_eig Xadj = (X.array() + min_).array().log();
    mean_ = Xadj.mean();
    Xadj.array() -= mean_;
    std_ = EigenUtil::StandardDeviation(Xadj);
  }
  fit_complete_ = true;
}

matrix_eig Normalizer::Transform(const matrix_eig &X) const {
  if (fit_complete_ && do_normalize_) {
    matrix_eig Xadj = (X.array() + min_).array().log();
    Xadj.array() -= mean_;
    return Xadj.array() / std_;
  } else if (do_normalize_) {
    throw("Please call `Fit` before `Transform` or `ReverseTransform`");
  } else {
    return X;
  }
}

matrix_eig Normalizer::ReverseTransform(const matrix_eig &X) const {
  if (fit_complete_ && do_normalize_) {
    matrix_eig Xadj = X.array() * std_ + mean_;
    return Xadj.array().exp() - min_;
  } else if (do_normalize_) {
    throw("Please call `Fit` before `Transform` or `ReverseTransform`");
  } else {
    return X;
  }
}

/**
 * BaseTFModel methods
 */
BaseTFModel::BaseTFModel(const std::string &modelgen_path,
                         const std::string &pymodel_path,
                         const std::string &graph_path)
    : BaseModel(),
      modelgen_path_(peloton::FileUtil::GetRelativeToRootPath(modelgen_path)),
      pymodel_path_(peloton::FileUtil::GetRelativeToRootPath(pymodel_path)),
      graph_path_(peloton::FileUtil::GetRelativeToRootPath(graph_path)) {
  tf_session_entity_ = std::unique_ptr<TfSessionEntity<float, float>>(
      new TfSessionEntity<float, float>());
  PELOTON_ASSERT(FileUtil::Exists(pymodel_path_));
}

BaseTFModel::~BaseTFModel() { remove(graph_path_.c_str()); }

void BaseTFModel::TFInit() {
  tf_session_entity_->Eval("init");
  PELOTON_ASSERT(tf_session_entity_->IsStatusOk());
}

void BaseTFModel::GenerateModel(const std::string &args_str) {
  std::string cmd = "python3 \"" + pymodel_path_ + "\" " + args_str;
  LOG_DEBUG("Executing command: %s", cmd.c_str());
  UNUSED_ATTRIBUTE bool succ = system(cmd.c_str());
  PELOTON_ASSERT(succ == 0);
  PELOTON_ASSERT(FileUtil::Exists(graph_path_));
}
}  // namespace brain
}  // namespace peloton