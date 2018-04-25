//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_interpreter.cpp
//
// Identification: src/codegen/interpreter/bytecode_interpreter.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/bytecode_interpreter.h"
#include "codegen/interpreter/bytecode_function.h"

namespace peloton {
namespace codegen {
namespace interpreter {

/** This is the actual dispatch code: It lookups the destination handler address
 *  in the label_pointers_ array and performs a direct jump there.
 */
#define INTERPRETER_DISPATCH_GOTO(ip)                   \
  goto *(label_pointers_[BytecodeFunction::GetOpcodeId( \
      reinterpret_cast<const Instruction *>(ip)->op)])

/**
 * The array with the label pointers has to be zero initialized to make sure,
 * that we fill it with the actual values on the first execution.
 */
void *
    BytecodeInterpreter::label_pointers_[BytecodeFunction::GetNumberOpcodes()] =
        {nullptr};

BytecodeInterpreter::BytecodeInterpreter(
    const BytecodeFunction &bytecode_function)
    : bytecode_function_(bytecode_function) {}

value_t BytecodeInterpreter::ExecuteFunction(
    const BytecodeFunction &bytecode_function,
    const std::vector<value_t> &arguments) {
  BytecodeInterpreter interpreter(bytecode_function);
  interpreter.ExecuteFunction(arguments);

  return interpreter.GetReturnValue<value_t>();
}

void BytecodeInterpreter::ExecuteFunction(
    const BytecodeFunction &bytecode_function, char *param) {
  BytecodeInterpreter interpreter(bytecode_function);
  interpreter.ExecuteFunction({reinterpret_cast<value_t &>(param)});
}

NEVER_INLINE NO_CLONE void BytecodeInterpreter::ExecuteFunction(
    const std::vector<value_t> &arguments) {
  // Fill the label_pointers_ array with the handler addresses at first
  // startup. (This can't be done outside of this function, as the labels are
  // not visible there.
  if (label_pointers_[0] == nullptr) {
#define HANDLE_INST(op) \
  label_pointers_[BytecodeFunction::GetOpcodeId(Opcode::op)] = &&_##op;

#include "codegen/interpreter/bytecode_instructions.def"
  }

  InitializeActivationRecord(arguments);

  // Get initial instruction pointer
  const Instruction *bytecode =
      reinterpret_cast<const Instruction *>(&bytecode_function_.bytecode_[0]);
  const Instruction *ip = bytecode;

  // Start execution with first instruction
  INTERPRETER_DISPATCH_GOTO(ip);

//--------------------------------------------------------------------------//
//                             Dispatch area
//
// This is the actual dispatch area of the interpreter. Because we use
// threaded interpretation, this is not a dispatch loop, but a long list of
// labels, and the control flow jumps from one handler to the next with
// goto's -> INTERPRETER_DISPATCH_GOTO(ip)
//
// The whole dispatch area gets generated using the bytecode_instructions.def
// file. All instruction handlers from query_interpreter.h will get inlined
// here for all their types. Even though the function looks small here,
// it will be over 13kB in the resulting binary!
//--------------------------------------------------------------------------//

#ifdef LOG_TRACE_ENABLED
#define TRACE_CODE_PRE LOG_TRACE("%s", bytecode_function_.Dump(ip).c_str())
#else
#define TRACE_CODE_PRE
#endif

#define HANDLE_RET_INST(op)                                       \
  _ret:                                                           \
  TRACE_CODE_PRE;                                                 \
  GetValueReference<value_t>(0) = GetValue<value_t>(ip->args[0]); \
  return;

#define HANDLE_TYPED_INST(op, type) \
  _##op##_##type : TRACE_CODE_PRE;  \
  ip = op##Handler<type>(ip);       \
  INTERPRETER_DISPATCH_GOTO(ip);

#define HANDLE_INST(op)   \
  _##op : TRACE_CODE_PRE; \
  ip = op##Handler(ip);   \
  INTERPRETER_DISPATCH_GOTO(ip);

#define HANDLE_EXPLICIT_CALL_INST(op, func) \
  _##op : TRACE_CODE_PRE;                   \
  ip = explicit_callHandler(ip, &func);     \
  INTERPRETER_DISPATCH_GOTO(ip);

#include "codegen/interpreter/bytecode_instructions.def"

  //--------------------------------------------------------------------------//
}

template <typename type_t>
type_t BytecodeInterpreter::GetReturnValue() {
  // the ret instruction saves the return value in value slot 0 by definition
  return GetValue<type_t>(0);
}

void BytecodeInterpreter::InitializeActivationRecord(
    const std::vector<value_t> &arguments) {
  // resize vector to required number of value slots
  values_.resize(bytecode_function_.number_values_);

  index_t value_slot = 1;

  // fill in constants
  for (auto &constant : bytecode_function_.constants_) {
    SetValue<value_t>(value_slot++, constant);
  }

  // check if provided number of arguments matches the number required by
  // the function
  if (bytecode_function_.number_function_arguments_ != arguments.size()) {
    throw Exception(
        "llvm function called through interpreter with wrong number of "
        "arguments");
  }

  // fill in function arguments
  for (auto &argument : arguments) {
    SetValue<value_t>(value_slot++, argument);
  }

  // prepare call activations
  call_activations_.resize(bytecode_function_.external_call_contexts_.size());
  for (size_t i = 0; i < bytecode_function_.external_call_contexts_.size();
       i++) {
    auto &call_context = bytecode_function_.external_call_contexts_[i];
    auto &call_activation = call_activations_[i];

    // initialize libffi call interface
    if (ffi_prep_cif(&call_activation.call_interface, FFI_DEFAULT_ABI,
                     call_context.args.size(), call_context.dest_type,
                     const_cast<ffi_type **>(call_context.arg_types.data())) !=
        FFI_OK) {
      throw Exception("initializing ffi call interface failed ");
    }

    // save the pointers to the value slots in the continuous arrays
    for (const auto &arg : call_context.args) {
      call_activation.value_pointers.push_back(&values_[arg]);
    }
    call_activation.return_pointer = &values_[call_context.dest_slot];
  }
}

uintptr_t BytecodeInterpreter::AllocateMemory(size_t number_bytes) {
  // allocate memory
  std::unique_ptr<char[]> pointer =
      std::unique_ptr<char[]>(new char[number_bytes]);

  // get raw pointer before moving pointer object!
  auto raw_pointer = reinterpret_cast<uintptr_t>(pointer.get());

  allocations_.emplace_back(std::move(pointer));
  return raw_pointer;
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton