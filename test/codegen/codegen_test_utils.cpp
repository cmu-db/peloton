//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// codegen_test_utils.cpp
//
// Identification: test/codegen/codegen_test_utils.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen_test_utils.h"

#include "type/value_peeker.h"
#include "codegen/runtime_functions_proxy.h"
#include "codegen/values_runtime_proxy.h"

namespace peloton {
namespace test {

expression::ConstantValueExpression *
CodegenTestUtils::CreateConstantIntExpression(int64_t val) {
  return new expression::ConstantValueExpression(
      ValueFactory::GetIntegerValue(val));
}

//===----------------------------------------------------------------------===//
// Buffer the tuple into the output buffer in the state
//===----------------------------------------------------------------------===//
void BufferingConsumer::BufferTuple(char *state, Value *vals,
                                    uint32_t num_vals) {
  BufferingState *buffer_state = reinterpret_cast<BufferingState *>(state);
  buffer_state->output->emplace_back(vals, num_vals);
}

//===----------------------------------------------------------------------===//
// Buffer the tuple into the output buffer in the state
//===----------------------------------------------------------------------===//
llvm::Function *BufferingConsumer::_BufferTupleProxy::GetFunction(
    codegen::CodeGen &codegen) {
  const std::string &fn_name =
#ifdef __APPLE__
      "_ZN7peloton4test17BufferingConsumer11bufferTupleEPcPNS_5ValueEj";
#else
      "_ZN7peloton4test17BufferingConsumer11BufferTupleEPcPNS_5ValueEj";
#endif

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> args = {
      codegen.CharPtrType(),
      codegen::ValuesRuntimeProxy::GetType(codegen)->getPointerTo(),
      codegen.Int32Type()};
  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

void BufferingConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  consumer_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.CharPtrType());
}

void BufferingConsumer::PrepareResult(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  tuple_buffer_ =
      codegen->CreateAlloca(codegen::ValuesRuntimeProxy::GetType(codegen),
                            codegen.Const32(ais_.size()));
}

//===----------------------------------------------------------------------===//
// Here we construct/stitch the tuple, then call
// BufferingConsumer::BufferTuple()
//===----------------------------------------------------------------------===//
void BufferingConsumer::ConsumeResult(codegen::ConsumerContext &ctx,
                                      codegen::RowBatch::Row &row) const {
  auto &codegen = ctx.GetCodeGen();
  for (size_t i = 0; i < ais_.size(); i++) {
    codegen::Value val = row.GetAttribute(codegen, ais_[i]);
    switch (val.GetType()) {
      case VALUE_TYPE_TINYINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputTinyInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case VALUE_TYPE_SMALLINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputSmallInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case VALUE_TYPE_DATE:
      case VALUE_TYPE_INTEGER: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputInteger::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case VALUE_TYPE_TIMESTAMP:
      case VALUE_TYPE_BIGINT: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputBigInt::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case VALUE_TYPE_DOUBLE: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputDouble::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue()});
        break;
      }
      case VALUE_TYPE_VARCHAR: {
        codegen.CallFunc(
            codegen::ValuesRuntimeProxy::_outputVarchar::GetFunction(codegen),
            {tuple_buffer_, codegen.Const64(i), val.GetValue(),
             val.GetLength()});
        break;
      }
      default: {
        throw Exception{"Can't serialize type " +
                        ValueTypeToString(val.GetType()) + " at position " +
                        std::to_string(i)};
      }
    }
  }

  // Append the tuple to the output buffer
  codegen.CallFunc(
      _BufferTupleProxy::GetFunction(codegen),
      {GetConsumerState(ctx), tuple_buffer_, codegen.Const32(ais_.size())});
}

//===----------------------------------------------------------------------===//
// Here we construct the printf string format of the tuple, then call printf()
//===----------------------------------------------------------------------===//
void Printer::ConsumeResult(codegen::ConsumerContext &ctx,
                            codegen::RowBatch::Row &row) const {
  codegen::CodeGen &codegen = ctx.GetCodeGen();

  // Iterate over the attributes, constructing the printf format along the way
  std::string format = "[";
  std::vector<llvm::Value *> cols;
  bool first = true;
  for (const auto *ai : ais_) {
    // Handle row format
    if (!first) {
      format.append(", ");
    }
    first = false;
    codegen::Value val = row.GetAttribute(codegen, ai);
    assert(val.GetType() != VALUE_TYPE_INVALID);
    switch (val.GetType()) {
      case VALUE_TYPE_BOOLEAN:
      case VALUE_TYPE_TINYINT:
      case VALUE_TYPE_SMALLINT:
      case VALUE_TYPE_DATE:
      case VALUE_TYPE_INTEGER: {
        format.append("%d");
        break;
      }
      case VALUE_TYPE_TIMESTAMP:
      case VALUE_TYPE_BIGINT: {
        format.append("%ld");
        break;
      }
      case VALUE_TYPE_DOUBLE: {
        format.append("%lf");
        break;
      }
      case VALUE_TYPE_VARCHAR: {
        cols.push_back(val.GetLength());
        format.append("'%.*s'");
        break;
      }
      default:
        throw std::runtime_error("Can't print that shit, bruh");
    }
    cols.push_back(val.GetValue());
  }
  format.append("]\n");

  // Make the printf call
  codegen.CallPrintf(format, cols);
}

void CountingConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  counter_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.Int64Type());
}

void CountingConsumer::InitializeState(codegen::CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto *state_ptr = GetCounter(codegen, context.GetRuntimeState());
  codegen->CreateStore(codegen.Const64(0), state_ptr);
}

// Increment the counter
void CountingConsumer::ConsumeResult(codegen::ConsumerContext &context,
                                     codegen::RowBatch::Row &) const {
  auto &codegen = context.GetCodeGen();

  auto *counter_ptr = GetCounter(context);
  auto *new_count =
      codegen->CreateAdd(codegen->CreateLoad(counter_ptr), codegen.Const64(1));
  codegen->CreateStore(new_count, counter_ptr);
}

}  // namespace test
}  // namespace peloton