//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen_consumer.cpp
//
// Identification: test/codegen/codegen_consumer.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen_consumer.h"

#include "type/value_factory.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/values_runtime_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

//===----------------------------------------------------------------------===//
// Buffer the tuple into the output buffer in the state
//===----------------------------------------------------------------------===//
void CodegenConsumer::BufferTuple(char *state, type::Value *vals,
                                    uint32_t num_vals) {
  BufferingState *buffer_state = reinterpret_cast<BufferingState *>(state);
  buffer_state->output->emplace_back(vals, num_vals);
}

//===----------------------------------------------------------------------===//
// Buffer the tuple into the output buffer in the state
//===----------------------------------------------------------------------===//
llvm::Function *CodegenConsumer::_BufferTupleProxy::GetFunction(
    codegen::CodeGen &codegen) {
  const std::string &fn_name =
#ifdef __APPLE__
      "_ZN7peloton7codegen15CodegenConsumer11BufferTupleEPcPNS_4type5ValueEj";
#else
      "_ZN7peloton7codegen15CodegenConsumer11BufferTupleEPcPNS_4type5ValueEj";
#endif

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> args = {
      codegen.CharPtrType(),
      codegen::ValueProxy::GetType(codegen)->getPointerTo(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

void CodegenConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  consumer_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.CharPtrType());
  // Introduce our output tuple buffer as local (on stack)
  auto *value_type = codegen::ValueProxy::GetType(codegen);
  tuple_output_state_id_ = runtime_state.RegisterState(
      "output", codegen.VectorType(value_type, ais_.size()), true);
}

//void CodegenConsumer::PrepareResult(codegen::CompilationContext &ctx) {
//  auto &codegen = ctx.GetCodeGen();
//  tuple_buffer_ =
//      codegen->CreateAlloca(codegen::ValueProxy::GetType(codegen),
//                            codegen.Const32(ais_.size()));
//}

//===----------------------------------------------------------------------===//
// Here we construct/stitch the tuple, then call
// CodegenConsumer::BufferTuple()
//===----------------------------------------------------------------------===//
void CodegenConsumer::ConsumeResult(codegen::ConsumerContext &ctx,
                                      codegen::RowBatch::Row &row) const {
  auto &codegen = ctx.GetCodeGen();
  auto *tuple_buffer_ = GetStateValue(ctx, tuple_output_state_id_);

  for (size_t i = 0; i < ais_.size(); i++) {
    codegen::Value val = row.GetAttribute(codegen, ais_[i]);
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP:
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue(),
             val.GetLength()});
        break;
      }
      default: {
        throw Exception{"Can't serialize type " +
                        TypeIdToString(val.GetType()) + " at position " +
                        std::to_string(i)};
      }
    }
  }

  // Append the tuple to the output buffer (by calling BufferTuple(...))
  std::vector<llvm::Value *> args = {GetStateValue(ctx, consumer_state_id_),
                                     tuple_buffer_,
                                     codegen.Const32(ais_.size())};
  codegen.CallFunc(_BufferTupleProxy::GetFunction(codegen), args);

}

}  // namespace test
}  // namespace peloton
