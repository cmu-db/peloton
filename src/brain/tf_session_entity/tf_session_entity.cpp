//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity.cpp
//
// Identification: src/brain/tf_session_entity/tf_session_entity.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "brain/tf_session_entity/tf_session_entity.h"
#include "brain/tf_session_entity/tf_session_entity_input.h"
#include "brain/tf_session_entity/tf_session_entity_output.h"

namespace peloton {
namespace brain {

/**
 * Constructor/Desctructor
 **/

TFSE_TEMPLATE_ARGUMENTS
TFSE_TYPE::TfSessionEntity() {
  graph_ = TF_NewGraph();
  status_ = TF_NewStatus();
  session_options_ = TF_NewSessionOptions();
  session_ = TF_NewSession(graph_, session_options_, status_);
}

TFSE_TEMPLATE_ARGUMENTS
TFSE_TYPE::~TfSessionEntity() {
  TF_CloseSession(session_, status_);
  TF_DeleteSession(session_, status_);
  TF_DeleteGraph(graph_);
  TF_DeleteStatus(status_);
  TF_DeleteSessionOptions(session_options_);
}

/*
 ********
 * Graph Import Utilities
 ********
 */

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::FreeBuffer(void *data, UNUSED_ATTRIBUTE size_t length) {
  ::operator delete(data);
}

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::ImportGraph(const std::string &filename) {
  TF_Buffer *graph_def = ReadFile(filename);
  TF_ImportGraphDefOptions *opts = TF_NewImportGraphDefOptions();
  TF_GraphImportGraphDef(graph_, graph_def, opts, status_);
  TF_DeleteImportGraphDefOptions(opts);
  TF_DeleteBuffer(graph_def);
  PELOTON_ASSERT(IsStatusOk());
}

TFSE_TEMPLATE_ARGUMENTS
TF_Buffer *TFSE_TYPE::ReadFile(const std::string &filename) {
  FILE *f = fopen(filename.c_str(), "rb");
  fseek(f, 0, SEEK_END);
  size_t fsize = (size_t)ftell(f);
  fseek(f, 0, SEEK_SET);  // same as rewind(f);
  // Reference:
  // https://stackoverflow.com/questions/14111900/using-new-on-void-pointer
  void *data = ::operator new(fsize);
  UNUSED_ATTRIBUTE size_t size_read = fread(data, fsize, 1, f);
  fclose(f);

  TF_Buffer *buf = TF_NewBuffer();
  buf->data = data;
  buf->length = fsize;
  buf->data_deallocator = TfSessionEntity::FreeBuffer;
  return buf;
}

/*
 ********
 * Evaluation/Session.Run()
 ********
 */

// Evaluate op with no inputs/outputs
TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::Eval(const std::string &opName) {
  TF_Operation *op = TF_GraphOperationByName(graph_, opName.c_str());
  TF_SessionRun(session_, nullptr, nullptr, nullptr, 0,  // inputs
                nullptr, nullptr, 0,                     // outputs
                &op, 1,                                  // targets
                nullptr, status_);
}

// Evaluate op with inputs and outputs
TFSE_TEMPLATE_ARGUMENTS
OutputType *TFSE_TYPE::Eval(
    const std::vector<TfSessionEntityInput<InputType>>& helper_inputs,
    const std::vector<TfSessionEntityOutput<OutputType>>& helper_outputs) {
  std::vector<TF_Tensor *> in_vals, out_vals;
  std::vector<TF_Output> ins, outs;
  for (auto helperIn : helper_inputs) {
    ins.push_back(
        {TF_GraphOperationByName(graph_, helperIn.GetPlaceholderName().c_str()),
         0});
    in_vals.push_back(helperIn.GetTensor());
  }
  for (auto helperOut : helper_outputs) {
    outs.push_back({TF_GraphOperationByName(
                        graph_, helperOut.GetPlaceholderName().c_str()),
                    0});
    out_vals.push_back(helperOut.GetTensor());
  }
  TF_SessionRun(session_, nullptr, &(ins.at(0)), &(in_vals.at(0)),
                ins.size(),                                     // Inputs
                &(outs.at(0)), &(out_vals.at(0)), outs.size(),  // Outputs
                nullptr, 0,                                     // Operations
                nullptr, status_);
  PELOTON_ASSERT(TF_GetCode(status_) == TF_OK);
  return static_cast<OutputType *>(TF_TensorData(out_vals.at(0)));
}

// Evaluate op with only inputs(where nothing is output eg. Backprop)
TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::Eval(const std::vector<TfSessionEntityInput<InputType>>& helper_inputs,
                     const std::string &op_name) {
  std::vector<TF_Tensor *> in_vals;
  std::vector<TF_Output> ins;
  for (auto helperIn : helper_inputs) {
    ins.push_back(
        {TF_GraphOperationByName(graph_, helperIn.GetPlaceholderName().c_str()),
         0});
    in_vals.push_back(helperIn.GetTensor());
  }
  TF_Operation *op = TF_GraphOperationByName(graph_, op_name.c_str());
  TF_SessionRun(session_, nullptr, &(ins.at(0)), &(in_vals.at(0)),
                ins.size(),           // Inputs
                nullptr, nullptr, 0,  // Outputs
                &op, 1,               // Operations
                nullptr, status_);
  PELOTON_ASSERT(TF_GetCode(status_) == TF_OK);
}

/*
 ********
 * Helper Operations
 ********
 */

TFSE_TEMPLATE_ARGUMENTS
void TFSE_TYPE::PrintOperations() {
  TF_Operation *oper;
  size_t pos = 0;
  std::string graph_ops = "Graph Operations List:";
  while ((oper = TF_GraphNextOperation(graph_, &pos)) != nullptr) {
    graph_ops += TF_OperationName(oper);
    graph_ops += "\n";
  }
  LOG_DEBUG("%s", graph_ops.c_str());
}

TFSE_TEMPLATE_ARGUMENTS
bool TFSE_TYPE::IsStatusOk() { return TF_GetCode(status_) == TF_OK; }

// Explicit template Initialization
template class TfSessionEntity<float, float>;
}  // namespace brain
}  // namespace peloton
