//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffering_consumer.cpp
//
// Identification: src/codegen/buffering_consumer.cpp
//
// Copyright (c) 2015-2017, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/buffering_consumer.h"

#include "codegen/lang/if.h"
#include "codegen/proxy/proxy.h"
#include "codegen/proxy/value_proxy.h"
#include "codegen/proxy/values_runtime_proxy.h"
#include "codegen/type/sql_type.h"
#include "planner/binding_context.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// WRAPPED TUPLE
//===----------------------------------------------------------------------===//

// Basic Constructor
WrappedTuple::WrappedTuple(peloton::type::Value *vals, uint32_t num_vals)
    : ContainerTuple(&tuple_), tuple_(vals, vals + num_vals) {}

// Copy Constructor
WrappedTuple::WrappedTuple(const WrappedTuple &o)
    : ContainerTuple(&tuple_), tuple_(o.tuple_) {}

WrappedTuple &WrappedTuple::operator=(const WrappedTuple &o) {
  ContainerTuple<std::vector<peloton::type::Value>>::operator=(o);
  tuple_ = o.tuple_;
  return *this;
}

//===----------------------------------------------------------------------===//
// BufferTuple() Proxy
//===----------------------------------------------------------------------===//

PROXY(BufferingConsumer) { DECLARE_METHOD(BufferTuple); };

DEFINE_METHOD(peloton::codegen, BufferingConsumer, BufferTuple);

//===----------------------------------------------------------------------===//
// BUFFERING CONSUMER
//===----------------------------------------------------------------------===//

BufferingConsumer::BufferingConsumer(const std::vector<oid_t> &cols,
                                     planner::BindingContext &context) {
  for (oid_t col_id : cols) {
    output_ais_.push_back(context.Find(col_id));
  }
  state.output = &tuples_;
}

// Append the array of values (i.e., a tuple) into the consumer's buffer of
// output tuples.
void BufferingConsumer::BufferTuple(char *state, char *tuple,
                                    uint32_t num_cols) {
  BufferingState *buffer_state = reinterpret_cast<BufferingState *>(state);
  buffer_state->output->emplace_back(
      reinterpret_cast<peloton::type::Value *>(tuple), num_cols);
}

// Create two pieces of state: a pointer to the output tuple vector and an
// on-stack value array representing a single tuple.
void BufferingConsumer::Prepare(CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  consumer_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.CharPtrType());

  // Introduce our output tuple buffer as local (on stack)
  auto *value_type = ValueProxy::GetType(codegen);
  tuple_output_state_id_ = runtime_state.RegisterState(
      "output", codegen.ArrayType(value_type, output_ais_.size()), true);
}

// For each output attribute, we write out the attribute's value into the
// currently active output tuple. When all attributes have been written, we
// call BufferTuple(...) to append the currently active tuple into the output.
void BufferingConsumer::ConsumeResult(ConsumerContext &ctx,
                                      RowBatch::Row &row) const {
  auto &codegen = ctx.GetCodeGen();
  auto *tuple_buffer_ = GetStateValue(ctx, tuple_output_state_id_);
  tuple_buffer_ =
      codegen->CreatePointerCast(tuple_buffer_, codegen.CharPtrType());

  for (size_t i = 0; i < output_ais_.size(); i++) {
    // Derive the column's final value
    Value val = row.DeriveValue(codegen, output_ais_[i]);

    PL_ASSERT(output_ais_[i]->type == val.GetType());
    const auto &sql_type = val.GetType().GetSqlType();

    // Check if it's NULL
    Value null_val;
    lang::If val_is_null{codegen, val.IsNull(codegen)};
    {
      // If the value is NULL (i.e., has the NULL bit set), produce the NULL
      // value for the given type.
      null_val = sql_type.GetNullValue(codegen);
    }
    val_is_null.EndIf();
    val = val_is_null.BuildPHI(null_val, val);

    // Output the value using the type's output function
    auto *output_func = sql_type.GetOutputFunction(codegen, val.GetType());

    // Setup the function arguments
    std::vector<llvm::Value *> args = {tuple_buffer_, codegen.Const32(i),
                                       val.GetValue()};
    if (val.GetLength() != nullptr) args.push_back(val.GetLength());

    // Call the function
    codegen.CallFunc(output_func, args);
  }

  // Append the tuple to the output buffer (by calling BufferTuple(...))
  auto *consumer_state = GetStateValue(ctx, consumer_state_id_);
  std::vector<llvm::Value *> args = {consumer_state, tuple_buffer_,
                                     codegen.Const32(output_ais_.size())};
  codegen.Call(BufferingConsumerProxy::BufferTuple, args);
}

}  // namespace codegen
}  // namespace peloton
