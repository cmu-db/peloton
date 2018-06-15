//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_function.cpp
//
// Identification: src/codegen/interpreter/bytecode_function.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/bytecode_function.h"

#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>

#include "codegen/codegen.h"

// Includes for explicit function calls
#include "codegen/bloom_filter_accessor.h"
#include "codegen/util/bloom_filter.h"
#include "codegen/buffering_consumer.h"
#include "codegen/deleter.h"
#include "codegen/inserter.h"
#include "codegen/query_parameters.h"
#include "codegen/runtime_functions.h"
#include "codegen/transaction_runtime.h"
#include "codegen/updater.h"
#include "codegen/util/oa_hash_table.h"
#include "codegen/util/hash_table.h"
#include "codegen/util/sorter.h"
#include "codegen/values_runtime.h"
#include "executor/executor_context.h"
#include "function/date_functions.h"
#include "function/numeric_functions.h"
#include "function/string_functions.h"
#include "function/timestamp_functions.h"
#include "planner/project_info.h"
#include "storage/data_table.h"
#include "storage/storage_manager.h"
#include "storage/tile_group.h"
#include "storage/zone_map_manager.h"
#include "codegen/util/buffer.h"

namespace peloton {
namespace codegen {
namespace interpreter {

/**
 * This lambda function serves as an init function to fill the const mapping
 * of function names to opcodes.
 */
const std::unordered_map<std::string, Opcode>
    BytecodeFunction::explicit_call_opcode_mapping_ = []() {
      std::unordered_map<std::string, Opcode> mapping;

#define HANDLE_INST(op)
#define HANDLE_EXPLICIT_CALL_INST(op, func) mapping[#func] = Opcode::op;

#include "codegen/interpreter/bytecode_instructions.def"

      return mapping;
    }();

const char *BytecodeFunction::GetOpcodeString(Opcode opcode) {
  switch (opcode) {
#define HANDLE_INST(opcode) \
  case Opcode::opcode:      \
    return #opcode;

#include "codegen/interpreter/bytecode_instructions.def"

    default:
      return "(invalid)";
  }
}

#ifndef NDEBUG
const llvm::Instruction *BytecodeFunction::GetIRInstructionFromIP(
    index_t instr_slot) const {
  return instruction_trace_.at(instr_slot);
}
#endif

size_t BytecodeFunction::GetInstructionSlotSize(
    const Instruction *instruction) {
  switch (instruction->op) {
#define HANDLE_INST(op) \
  case Opcode::op:      \
    return 1;
#define HANDLE_EXTERNAL_CALL_INST(op) \
  case Opcode::op:                    \
    return 2;
#define HANDLE_INTERNAL_CALL_INST(op)         \
  case Opcode::op:                            \
    return GetInteralCallInstructionSlotSize( \
        reinterpret_cast<const InternalCallInstruction *>(instruction));
#define HANDLE_SELECT_INST(op) \
  case Opcode::op:             \
    return 2;
#define HANDLE_OVERFLOW_TYPED_INST(op, type) \
  case Opcode::op##_##type:                  \
    return 2;
#define HANDLE_EXPLICIT_CALL_INST(op, func)    \
  case Opcode::op:                             \
    return GetExplicitCallInstructionSlotSize( \
        GetFunctionRequiredArgSlotsNum(&func));

#include "codegen/interpreter/bytecode_instructions.def"

    default:
      PELOTON_ASSERT(false);
      return 0;
  }
}

Opcode BytecodeFunction::GetExplicitCallOpcodeByString(
    std::string function_name) {
  auto result = explicit_call_opcode_mapping_.find(function_name);

  if (result != explicit_call_opcode_mapping_.end())
    return result->second;
  else
    return Opcode::undefined;
}

void BytecodeFunction::DumpContents() const {
  std::ofstream output;
  output.open(function_name_ + ".bf");

#ifndef NDEBUG
  const llvm::BasicBlock *bb;
#endif

  // Print Bytecode
  output << "Bytecode:" << std::endl;
  for (index_t i = 0; i < bytecode_.size();) {
    auto *instruction = GetIPFromIndex(i);

#ifndef NDEBUG
    const llvm::Instruction *llvm_instruction = GetIRInstructionFromIP(i);
    if (llvm_instruction->getOpcode() != llvm::Instruction::PHI) {
      if (i > 0 && bb != llvm_instruction->getParent()) {
        output << llvm_instruction->getParent()->getName().str() << ":"
               << std::endl;
      }
      bb = llvm_instruction->getParent();
    }
#endif

    output << Dump(instruction) << std::endl;
    i += GetInstructionSlotSize(instruction);
  }

  // Print Constants
  if (constants_.size() > 0) output << "Constants:" << std::endl;
  for (size_t i = 0; i < constants_.size(); i++) {
    output << "[" << std::setw(3) << std::dec << (i + 1)
           << "] = " << *reinterpret_cast<const int64_t *>(&constants_[i])
           << " 0x" << std::hex << constants_[i] << std::endl;
  }

  output << std::endl;

  output.close();
}

std::string BytecodeFunction::Dump(const Instruction *instruction) const {
  std::ostringstream output;
  output << "[" << std::setw(3) << GetIndexFromIP(instruction) << "] ";
  output << std::setw(18) << GetOpcodeString(instruction->op) << " ";

  switch (instruction->op) {
#define HANDLE_INST(opcode)                                        \
  case Opcode::opcode:                                             \
    output << "[" << std::setw(3) << instruction->args[0] << "] "; \
    output << "[" << std::setw(3) << instruction->args[1] << "] "; \
    output << "[" << std::setw(3) << instruction->args[2] << "] "; \
    break;

#ifndef NDEBUG
#define HANDLE_EXTERNAL_CALL_INST(opcode)                                      \
  case Opcode::opcode:                                                         \
    output                                                                     \
        << "[" << std::setw(3)                                                 \
        << external_call_contexts_                                             \
               [reinterpret_cast<const ExternalCallInstruction *>(instruction) \
                    ->external_call_context]                                   \
                   .dest_slot                                                  \
        << "] ";                                                               \
    for (auto arg : external_call_contexts_[instruction->args[0]].args) {      \
      output << "[" << std::setw(3) << arg << "] ";                            \
    }                                                                          \
    output << "("                                                              \
           << static_cast<const llvm::CallInst *>(                             \
                  instruction_trace_[GetIndexFromIP(instruction)])             \
                  ->getCalledFunction()                                        \
                  ->getName()                                                  \
                  .str()                                                       \
           << ") ";                                                            \
    break;
#else
#define HANDLE_CALL_INST(opcode)                                        \
  case Opcode::opcode:                                                  \
    output << "[" << std::setw(3)                                       \
           << call_contexts_[reinterpret_cast<const CallInstruction *>( \
                                 instruction)                           \
                                 ->call_context]                        \
                  .dest_slot                                            \
           << "] ";                                                     \
    for (auto arg : call_contexts_[instruction->args[0]].args) {        \
      output << "[" << std::setw(3) << arg << "] ";                     \
    }                                                                   \
    break;
#endif

#ifndef NDEBUG
#define HANDLE_INTERNAL_CALL_INST(opcode)                                      \
  case Opcode::opcode:                                                         \
    output << "[" << std::setw(3)                                              \
           << reinterpret_cast<const InternalCallInstruction *>(instruction)   \
                  ->dest_slot                                                  \
           << "] ";                                                            \
    for (size_t i = 0;                                                         \
         i < reinterpret_cast<const InternalCallInstruction *>(instruction)    \
                 ->number_args;                                                \
         i++) {                                                                \
      output << "[" << std::setw(3)                                            \
             << reinterpret_cast<const InternalCallInstruction *>(instruction) \
                    ->args[i]                                                  \
             << "] ";                                                          \
    }                                                                          \
    output << "("                                                              \
           << static_cast<const llvm::CallInst *>(                             \
                  instruction_trace_[GetIndexFromIP(instruction)])             \
                  ->getCalledFunction()                                        \
                  ->getName()                                                  \
                  .str()                                                       \
           << ") ";                                                            \
    break;
#else
#define HANDLE_INTERNAL_CALL_INST(opcode)                                      \
  case Opcode::opcode:                                                         \
    output << "[" << std::setw(3)                                              \
           << reinterpret_cast<const InternalCallInstruction *>(instruction)   \
                  ->dest_slot                                                  \
           << "] ";                                                            \
    for (size_t i = 0;                                                         \
         i < reinterpret_cast<const InternalCallInstruction *>(instruction)    \
                 ->number_args;                                                \
         i++) {                                                                \
      output << "[" << std::setw(3)                                            \
             << reinterpret_cast<const InternalCallInstruction *>(instruction) \
                    ->args[i]                                                  \
             << "] ";                                                          \
    }                                                                          \
    break;
#endif

#define HANDLE_SELECT_INST(opcode)                                 \
  case Opcode::opcode:                                             \
    output << "[" << std::setw(3) << instruction->args[0] << "] "; \
    output << "[" << std::setw(3) << instruction->args[1] << "] "; \
    output << "[" << std::setw(3) << instruction->args[2] << "] "; \
    output << "[" << std::setw(3) << instruction->args[3] << "] "; \
    break;

#define HANDLE_OVERFLOW_TYPED_INST(op, type)                       \
  case Opcode::op##_##type:                                        \
    output << "[" << std::setw(3) << instruction->args[0] << "] "; \
    output << "[" << std::setw(3) << instruction->args[1] << "] "; \
    output << "[" << std::setw(3) << instruction->args[2] << "] "; \
    output << "[" << std::setw(3) << instruction->args[3] << "] "; \
    break;

#define HANDLE_EXPLICIT_CALL_INST(opcode, func)                        \
  case Opcode::opcode:                                                 \
    for (size_t i = 0; i < GetFunctionRequiredArgSlotsNum(&func); i++) \
      output << "[" << std::setw(3) << instruction->args[i] << "] ";   \
    break;

#include "codegen/interpreter/bytecode_instructions.def"

    default:
      break;
  }

#ifndef NDEBUG
  output << "("
         << CodeGen::Dump(GetIRInstructionFromIP(GetIndexFromIP(instruction)))
         << ")";
#endif

  return output.str();
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
