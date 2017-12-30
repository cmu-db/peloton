//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// counting_consumer.h
//
// Identification: src/include/codegen/counting_consumer.h
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/query_result_consumer.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// A consumer that just counts the number of results
//===----------------------------------------------------------------------===//
class CountingConsumer : public codegen::QueryResultConsumer {
 public:
  void Prepare(codegen::CompilationContext &compilation_context) override;
  void InitializeState(codegen::CompilationContext &context) override;
  void ConsumeResult(codegen::ConsumerContext &context,
                     codegen::RowBatch::Row &row) const override;
  void TearDownState(codegen::CompilationContext &) override {}

  uint64_t GetCount() const { return counter_; }
  void ResetCount() { counter_ = 0; }
  char *GetConsumerState() final { return reinterpret_cast<char *>(&counter_); }

 private:
  llvm::Value *GetCounterState(codegen::CodeGen &codegen,
                               codegen::RuntimeState &runtime_state) const;

 private:
  uint64_t counter_;
  // The slot in the runtime state to find our state context
  codegen::RuntimeState::StateID counter_state_id_;
};

}  // namespace codegen
}  // namespace peloton
