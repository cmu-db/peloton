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
#include "util/file_util.h"
#include "brain/util/tf_session_entity/tf_session_entity.h"
#include "brain/util/tf_session_entity/tf_session_entity_input.h"
#include "brain/util/tf_session_entity/tf_session_entity_output.h"

namespace peloton {
namespace brain {

void BaseForecastModel::GetBatch(const matrix_eig &mat, size_t batch_offset,
                                 size_t bsz, size_t bptt,
                                 std::vector<matrix_eig> &data,
                                 std::vector<matrix_eig> &target) const {
  size_t samples_per_input = mat.rows() / bsz;
  size_t seq_len =
      std::min<size_t>(bptt, samples_per_input - horizon_ - batch_offset);
  // bsz vector of <seq_len, feat_len> = (bsz, seq_len, feat_len)
  for (size_t input_idx = 0; input_idx < bsz; input_idx++) {
    size_t row_idx = input_idx * samples_per_input;
    // train mat[row_idx:row_idx + seq_len, :)
    matrix_eig data_batch = mat.block(row_idx, 0, seq_len, mat.cols());
    // target mat[row_idx + horizon_: row_idx + seq_len + horizon_ - 1, :]
    matrix_eig target_batch = mat.block(row_idx + horizon_, 0, seq_len, mat.cols());
    // Push batches into containers
    data.push_back(data_batch);
    target.push_back(target_batch);
  }
}

BaseTFModel::BaseTFModel(const std::string& modelgen_path,
                         const std::string& pymodel_path,
                         const std::string& graph_path)
    : BaseModel(),
      modelgen_path_(peloton::FileUtil::GetRelativeToRootPath(modelgen_path)),
      pymodel_path_(peloton::FileUtil::GetRelativeToRootPath(pymodel_path)),
      graph_path_(peloton::FileUtil::GetRelativeToRootPath(graph_path)) {
  tf_session_entity_ = std::unique_ptr<TfSessionEntity<float, float>>(
      new TfSessionEntity<float, float>());
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
}
}
}