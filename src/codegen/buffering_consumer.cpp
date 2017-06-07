//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// buffering_consumer.cpp
//
// Identification: src/codegen/buffering_consumer.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/buffering_consumer.h"

#include "include/codegen/proxy/values_runtime_proxy.h"
#include "include/codegen/proxy/value_proxy.h"
#include "common/logger.h"
#include "planner/binding_context.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// WRAPPED TUPLE
//===----------------------------------------------------------------------===//

// Basic Constructor
WrappedTuple::WrappedTuple(type::Value *vals, uint32_t num_vals)
    : ContainerTuple(&tuple_), tuple_(vals, vals + num_vals) {}

// Copy Constructor
WrappedTuple::WrappedTuple(const WrappedTuple &o)
    : ContainerTuple(&tuple_), tuple_(o.tuple_) {}

WrappedTuple &WrappedTuple::operator=(const WrappedTuple &o) {
  expression::ContainerTuple<std::vector<type::Value>>::operator=(o);
  tuple_ = o.tuple_;
  return *this;
}

//===----------------------------------------------------------------------===//
// RESULT BUFFERING CONSUMER
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
void BufferingConsumer::BufferTuple(char *state, type::Value *vals,
                                    uint32_t num_vals) {
  BufferingState *buffer_state = reinterpret_cast<BufferingState *>(state);
  buffer_state->output->emplace_back(vals, num_vals);
}

// Get a proxy to BufferingConsumer::BufferTuple(...)
llvm::Function *BufferingConsumer::_BufferTupleProxy::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name =
#ifdef __APPLE__
      "_ZN7peloton7codegen17BufferingConsumer11BufferTupleEPcPNS_4type5ValueEj";
#else
      "_ZN7peloton7codegen17BufferingConsumer11BufferTupleEPcPNS_4type5ValueEj";
#endif

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> args = {
      codegen.CharPtrType(), ValueProxy::GetType(codegen)->getPointerTo(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
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
      "output", codegen.VectorType(value_type, output_ais_.size()), true);
}

// For each output attribute, we write out the attribute's value into the
// currently active output tuple. When all attributes have been written, we
// call BufferTuple(...) to append the currently active tuple into the output.
void BufferingConsumer::ConsumeResult(ConsumerContext &ctx,
                                      RowBatch::Row &row) const {
  auto &codegen = ctx.GetCodeGen();
  auto *tuple_buffer_ = GetStateValue(ctx, tuple_output_state_id_);

  for (size_t i = 0; i < output_ais_.size(); i++) {
    Value val = row.DeriveValue(codegen, output_ais_[i]);
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARBINARY: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarbinary::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue(),
             val.GetLength()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue(),
             val.GetLength()});
        break;
      }
      default: {
        std::string msg =
            StringUtil::Format("Can't serialize value type '%s' at position %u",
                               TypeIdToString(val.GetType()).c_str(), i);
        throw Exception{msg};
      }
    }
  }

  // Append the tuple to the output buffer (by calling BufferTuple(...))
  std::vector<llvm::Value *> args = {GetStateValue(ctx, consumer_state_id_),
                                     tuple_buffer_,
                                     codegen.Const32(output_ais_.size())};
  codegen.CallFunc(_BufferTupleProxy::GetFunction(codegen), args);
}

}  // namespace test
}  // namespace peloton
