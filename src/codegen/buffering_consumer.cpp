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
                                     const planner::BindingContext &context) {
  for (oid_t col_id : cols) {
    output_ais_.push_back(context.Find(col_id));
  }
}

// Append the array of row attributes to the buffer of tuples
//
// Note: buffering consumers rely on an ugly mutex to protect access to the
//       output buffer. This is ugly AF. We don't actually use it for primary
//       query processing, so it's okay.
void BufferingConsumer::BufferTuple(char *opaque_state, char *tuple,
                                    uint32_t num_cols) {
  auto *buffer = reinterpret_cast<Buffer *>(opaque_state);
  std::lock_guard<std::mutex> lock{buffer->mutex};
  buffer->output.emplace_back(reinterpret_cast<peloton::type::Value *>(tuple),
                              num_cols);
}

// Create two pieces of state: a pointer to the output tuple vector and an
// on-stack value array representing a single tuple.
void BufferingConsumer::Prepare(CompilationContext &compilation_ctx) {
  // Be sure to call our parent
  ExecutionConsumer::Prepare(compilation_ctx);

  // Install a little char* for the state we need
  CodeGen &codegen = compilation_ctx.GetCodeGen();
  QueryState &query_state = compilation_ctx.GetQueryState();
  consumer_state_id_ =
      query_state.RegisterState("consumerState", codegen.CharPtrType());
}

// For each output attribute, we write out the attribute's value into the
// currently active output tuple. When all attributes have been written, we
// call BufferTuple(...) to append the currently active tuple into the output.
void BufferingConsumer::ConsumeResult(ConsumerContext &ctx,
                                      RowBatch::Row &row) const {
  CodeGen &codegen = ctx.GetCodeGen();

  auto num_cols = static_cast<uint32_t>(output_ais_.size());
  auto *tuple_buffer_ =
      codegen.AllocateBuffer(ValueProxy::GetType(codegen), num_cols, "output");
  tuple_buffer_ =
      codegen->CreatePointerCast(tuple_buffer_, codegen.CharPtrType());

  for (uint32_t i = 0; i < num_cols; i++) {
    // Derive the column's final value
    Value val = row.DeriveValue(codegen, output_ais_[i]);

    PELOTON_ASSERT(output_ais_[i]->type == val.GetType());
    const auto &sql_type = val.GetType().GetSqlType();

    // Check if it's NULL
    Value null_val;
    lang::If val_is_null(codegen, val.IsNull(codegen));
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
    // If the value is a string, push back the length
    if (val.GetLength() != nullptr) {
      args.push_back(val.GetLength());
    }

    // If the value is a boolean, push back the NULL bit. We don't do that for
    // the other data types because we have special values for NULL. Booleans
    // in codegen are 1-bit types, as opposed to 1-byte types in the rest of the
    // system. Since, we cannot have a special value for NULL in a 1-bit boolean
    // system, we pass along the NULL bit during output.
    if (sql_type.TypeId() == peloton::type::TypeId::BOOLEAN) {
      args.push_back(val.IsNull(codegen));
    }

    // Call the function
    codegen.CallFunc(output_func, args);
  }

  auto &query_state = ctx.GetQueryState();
  llvm::Value *buffer_ptr =
      query_state.LoadStateValue(codegen, consumer_state_id_);

  // Append the tuple to the output buffer (by calling BufferTuple(...))
  std::vector<llvm::Value *> args = {buffer_ptr, tuple_buffer_,
                                     codegen.Const32(output_ais_.size())};
  codegen.Call(BufferingConsumerProxy::BufferTuple, args);
}

char *BufferingConsumer::GetConsumerState() {
  return reinterpret_cast<char *>(&buffer_);
}

const std::vector<WrappedTuple> &BufferingConsumer::GetOutputTuples() const {
  return buffer_.output;
}

}  // namespace codegen
}  // namespace peloton
