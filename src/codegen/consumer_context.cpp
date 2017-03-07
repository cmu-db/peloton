//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// consumer_context.cpp
//
// Identification: src/codegen/consumer_context.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/consumer_context.h"

#include "codegen/compilation_context.h"
#include "codegen/expression_translator.h"
#include "codegen/operator_translator.h"

namespace peloton {
namespace codegen {

// Constructor
ConsumerContext::ConsumerContext(CompilationContext &compilation_context,
                                 Pipeline &pipeline)
    : compilation_context_(compilation_context), pipeline_(pipeline) {}

codegen::Value ConsumerContext::DeriveValue(
    const expression::AbstractExpression &expression, RowBatch::Row &row) {
  auto *exp_translator = compilation_context_.GetTranslator(expression);
  if (exp_translator == nullptr) {
    throw Exception{"No translator found"};
  }
  return exp_translator->DeriveValue(*this, row);
}

// Pass this consumer context to the parent of the caller of consume()
void ConsumerContext::Consume(RowBatch &batch) {
  auto *translator = pipeline_.NextStep();
  if (translator != nullptr) {
    translator->Consume(*this, batch);
  } else {
    // We're at the end of the query pipeline, we now send the output tuples
    // to the result consumer configured in the compilation context
    auto &consumer = compilation_context_.GetQueryResultConsumer();
    consumer.ConsumeResult(*this, batch);
  }
}

void ConsumerContext::Consume(RowBatch::Row &row) {
  if (pipeline_.AtStageBoundary()) {
    auto &codegen = GetCodeGen();
    row.SetValidity(codegen, codegen.ConstBool(true));
    return;
  }

  auto *translator = pipeline_.NextStep();
  if (translator != nullptr) {
    translator->Consume(*this, row);
  } else {
    // We're at the end of the query pipeline, we now send the output tuples
    // to the result consumer configured in the compilation context
    auto &consumer = compilation_context_.GetQueryResultConsumer();
    consumer.ConsumeResult(*this, row);
  }
}

CodeGen &ConsumerContext::GetCodeGen() const {
  return compilation_context_.GetCodeGen();
}

RuntimeState &ConsumerContext::GetRuntimeState() const {
  return compilation_context_.GetRuntimeState();
}

}  // namespace codegen
}  // namespace peloton