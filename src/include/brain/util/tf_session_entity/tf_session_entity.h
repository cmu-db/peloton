//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tf_session_entity.h
//
// Identification: src/include/brain/util/tf_session_entity/tf_session_entity.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "tf_session_entity_io.h"

#define TFSE_TEMPLATE_ARGUMENTS \
  template <typename InputType, typename OutputType>
#define TFSE_TYPE TfSessionEntity<InputType, OutputType>

namespace peloton {
namespace brain {

// Forward Declarations
template <class InputType>
class TfSessionEntityInput;
template <class OutputType>
class TfSessionEntityOutput;

/**
 * The `TfSessionEntity` class is the main entity class for
 * handling Tensorflow sessions and evaluations within.
 * Since creating a graph in Tensorflow C API is pretty challenging
 * it is recommended to generally create a graph in a more stable API(eg.
 * python)
 * and import its serialized(.pb) version for use here.
 * This class supports and makes the following simplified:
 * (1) Session initialization
 * (2) Graph Imports: Using `ImportGraph`
 * (3) Evaluation of Ops: using different versions of the `Eval` fn.
 * The inputs and outputs are simplified by using the wrapper classes
 * `TfSessionEntityInput`, `TfSessionEntityOutput`.
 * (4) Misc Helper operations
 */
TFSE_TEMPLATE_ARGUMENTS
class TfSessionEntity {
 public:
  TfSessionEntity();
  ~TfSessionEntity();

  // Graph Import
  void ImportGraph(const std::string &filename);

  /**
   * Evaluation/Session.Run() options.
   * Provides a set of options to evaluate an Op either with
   * 1. No input/outputs eg. Global variable initialization
   * 2. Inputs, but no outputs eg. Backpropagation
   * 3. Inputs and 1 Output eg. Loss calculation, Predictions.
   * 3 with multiple outputs is easy to setup and can be done if need arises
   * based on (3) itself
   */
  void Eval(const std::string &opName);
  void Eval(const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
            const std::string &op_name);
  OutputType *Eval(
      const std::vector<TfSessionEntityInput<InputType> *> &helper_inputs,
      TfSessionEntityOutput<OutputType> *helper_outputs);

  /** Helpers **/
  // Print the name of all operations(`ops`) in this graph
  void PrintOperations();
  /**
   * Checking if the status is ok - Tensorflow C is horrible in showing there's
   * an error
   * uptil the eval statement - adding status checks can help find in which step
   * the error
   * has occurred.
   */
  bool IsStatusOk();

 private:
  /**
   * Member variables to maintain state of the Session entity object
   * These variables are used throughout by functions of this class
   */
  TF_Graph *graph_;
  TF_Status *status_;
  TF_SessionOptions *session_options_;
  TF_Session *session_;

  // For Graph IO handling
  TF_Buffer *ReadFile(const std::string &filename);
  static void FreeBuffer(void *data, size_t length);
};
}  // namespace brain
}  // namespace peloton
