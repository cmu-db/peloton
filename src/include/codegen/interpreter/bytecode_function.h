//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_function.h
//
// Identification: src/include/codegen/interpreter/bytecode_function.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <ffi.h>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/macros.h"
#include "util/math_util.h"

namespace llvm {
class Instruction;
}  // namespace llvm

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {

class BytecodeInterpreter;
class BytecodeBuilder;

// Type definitions to match the LLVM terminology
using i8 = uint8_t;
using i16 = uint16_t;
using i32 = uint32_t;
using i64 = uint64_t;
using value_t = uint64_t;
using index_t = uint16_t;
using instr_slot_t = uint64_t;

// Type template that converts any type into the matching interpreter type
template <typename type>
using bytecode_type = typename std::conditional<
    sizeof(type) == 1, i8,
    typename std::conditional<
        sizeof(type) == 2, i16,
        typename std::conditional<sizeof(type) == 3 || sizeof(type) == 4, i32,
                                  i64>::type>::type>::type;

/**
 * Enum holding all Opcodes for all instructions.
 */
enum class Opcode : index_t {
  undefined,

#define HANDLE_INST(opcode) opcode,
#include "codegen/interpreter/bytecode_instructions.def"
#undef HANDLE_INST

  NUMBER_OPCODES
};

/**
 * Struct to access a generic bytecode instruction.
 * Every bytecode instruction starts with a 2 byte Opcode, followed by a
 * variable number of 2 byte arguments. (Exception: ExternalCallInstruction)
 *
 * This struct is only for accessing Instructions, not for saving them!
 * (sizeof returns a wrong value) All bytecode instructions are saved in
 * one or more 8 byte instructions slots (instr_slot_t) in the bytecode stream.
 */
struct Instruction {
  Opcode op;
  index_t args[];
};

/**
 * Specialized struct for accessing a InternalCallInstruction. The number of
 * arguments in .args[] is variable and must match the value .number_args .
 * GetInteralCallInstructionSlotSize uses this information to calculate the
 * number of occupied instruction slots.
 */
struct InternalCallInstruction {
  Opcode op;
  index_t sub_function;
  index_t dest_slot;
  index_t number_args;
  index_t args[];
};

/**
 * Specialized struct for accessing a ExternalCallInstruction. It is the only
 * instruction, that contains a field that is greater than 2 byte.
 * Because libffi requires pointers to value slots of the current
 * activation record, the instruction itself only contains an index for
 * accessing the proper call context. During interpretation a call activation
 * is created for every call context, holding the actual runtime pointers,
 * which can be accessed with the same index.
 */
struct ExternalCallInstruction {
  Opcode op;
  index_t external_call_context;
  void (*function)(void);
};

/**
 * Call context holding information needed to create a runtime call activation
 * for a ExternalCallInstruction in the bytecode stream.
 */
struct ExternalCallContext {
  index_t dest_slot;
  ffi_type *dest_type;
  std::vector<index_t> args;
  std::vector<ffi_type *> arg_types;
};

/**
 * A BytecodeFunction contains all information necessary to run a LLVM
 * function in the interpreter and is completely independent from the
 * CodeContext it was created from (except for the tracing information in debug
 * mode). It can be moved and copied.
 */
class BytecodeFunction {
 public:
  /**
   * Returns the Opcode enum for a given Opcode Id (to avoid plain casting)
   * @param id Opcode Id
   * @return Opcode enum
   */
  ALWAYS_INLINE inline static constexpr Opcode GetOpcodeFromId(index_t id) {
    return static_cast<Opcode>(id);
  }

  /**
   * Returns the Opcode Id to a given Opcode enum (to avoid plain casting)
   * @param opcode Opcode enum
   * @return Opcode Id
   */
  ALWAYS_INLINE inline static constexpr index_t GetOpcodeId(Opcode opcode) {
    return static_cast<index_t>(opcode);
  }

  /**
   * Returns a numan readable string to a given Opcode
   * @param opcode Opcode enum
   * @return String representation if the Opcode
   */
  static const char *GetOpcodeString(Opcode opcode);

  /**
   * Returns the overall number of existing Opcodes (not trivial, as the Opcodes
   * are created with expanding macros)
   * @return overall number of existing Opcodes
   */
  inline static constexpr size_t GetNumberOpcodes() {
    return static_cast<index_t>(Opcode::NUMBER_OPCODES);
  }

  /**
   * Return the instruction pointer to a given instruction index (from this
   * bytecode function)
   * @param index instruction index
   * @return pointer to the instruction at that index inside the bytecode
   */
  ALWAYS_INLINE inline const Instruction *GetIPFromIndex(index_t index) const {
    return reinterpret_cast<const Instruction *>(
        const_cast<instr_slot_t *>(bytecode_.data()) + index);
  }

  /**
   * Returns the instruction index for a given instruction pointer (from this
   * bytecode function)
   * @param instruction pointer to a given instruction inside the bytecode
   * @return index to the instruction the pointer is pointing to
   */
  ALWAYS_INLINE inline index_t GetIndexFromIP(
      const Instruction *instruction) const {
    index_t index =
        reinterpret_cast<const instr_slot_t *>(instruction) - bytecode_.data();
    return index;
  }

#ifndef NDEBUG
  const llvm::Instruction *GetIRInstructionFromIP(index_t instr_slot) const;
#endif

  /**
   * Returns the number of slots a given instruction occupies in the bytecode
   * stream.
   * @param instruction pointer to the instruction inside the bytecode
   * @return number of slots (each 8 Byte) that are used by this instruction
   */
  static size_t GetInstructionSlotSize(const Instruction *instruction);

  /**
   * Returns the number of slots a given internal call instruction occupies in
   * the bytecode stream. Internal instructions have a variable length, so the
   * size has to be calculated.
   * @param instruction pointer to instruction of type internal call
   * @return number of slots (each 8 Byte) that are used by this instruction
   */
  static ALWAYS_INLINE inline size_t GetInteralCallInstructionSlotSize(
      const InternalCallInstruction *instruction) {
    const size_t number_slots =
        MathUtil::DivRoundUp(sizeof(uint16_t) * (4 + instruction->number_args),
                             sizeof(instr_slot_t));
    PELOTON_ASSERT(number_slots > 0);
    return number_slots;
  }

  /**
   * Returns the number of slots an explicit call instruction occupies,
   * given the number of argument slots. (return value and/or object pointer
   * also need a slot!)
   * @param number_args number of needed argument slots
   * @return number of slots (each 8 Byte) that are used by this instruction
   */
  static constexpr ALWAYS_INLINE inline size_t
  GetExplicitCallInstructionSlotSize(size_t number_args) {
    return MathUtil::DivRoundUp(sizeof(uint16_t) * (1 + number_args),
                                sizeof(instr_slot_t));
  }

  /**
   * Returns the number of required argument slots that are needed in an
   * explicit call bytecode instruction for this function.
   * @param func pointer/reference to the function (declaration must be visible)
   * @return number of required argument slots
   *  = arguments + return value + object pointer
   */
  template <typename return_type, typename... arg_types>
  static constexpr ALWAYS_INLINE inline size_t GetFunctionRequiredArgSlotsNum(
      UNUSED_ATTRIBUTE return_type (*func)(arg_types...)) {
    return (std::is_void<return_type>::value) ? sizeof...(arg_types)
                                              : sizeof...(arg_types) + 1;
  }

  template <typename return_type, typename class_type, typename... arg_types>
  static constexpr ALWAYS_INLINE inline size_t GetFunctionRequiredArgSlotsNum(
      UNUSED_ATTRIBUTE return_type (class_type::*func)(arg_types...)) {
    return (std::is_void<return_type>::value) ? sizeof...(arg_types) + 1
                                              : sizeof...(arg_types) + 2;
  }

  template <typename return_type, typename class_type, typename... arg_types>
  static constexpr ALWAYS_INLINE inline size_t GetFunctionRequiredArgSlotsNum(
      UNUSED_ATTRIBUTE return_type (class_type::*func)(arg_types...) const) {
    return (std::is_void<return_type>::value) ? sizeof...(arg_types) + 1
                                              : sizeof...(arg_types) + 2;
  }

  /**
   * Returns the opcode for a fiven function name string (lookup in hash map).
   * @param function_name string of function name with namespace
   * @return the matching opcode or Opcode::undefined
   */
  static Opcode GetExplicitCallOpcodeByString(std::string function_name);

  /***
   * Dumps the bytecode and the constants of this bytecode function to a
   * file, identified by function name.
   */
  void DumpContents() const;

  /**
   * Gives a textual representation of the given instruction. (and the
   * LLVM instruction it originates from, if Debug mode is enabled)
   * @param instruction instruction from this bytecode function
   * @return string containing a textual representatino of the instruction
   */
  std::string Dump(const Instruction *instruction) const;

 private:
  /**
   * Creates a new empty BytecodeFunction object.
   * @param id identifier for this bytecode function, usually inherited
   * from code context.
   */
  BytecodeFunction(std::string function_name) : function_name_(function_name) {}

 private:
  /**
   * Function name of the original function (used only for output).
   */
  std::string function_name_;

  /**
   * Number of needed value slots at runtime.
   */
  size_t number_values_;

  /**
   * Number of function arguments (to check correct number given to interpreter)
   */
  size_t number_function_arguments_;

  /**
   * Constants needed during runtime.
   */
  std::vector<value_t> constants_;

  /**
   * This array of instruction slots holds the actual bytecode that is
   * interpreted. Usually one instruction occupies one slot, but some
   * instruction require several slots. Except for InternalCallInstruction,
   * all instruction have a static size. The number of occipied instruction
   * slots for an instruction can be obtained by GetInstructionSlotSize()
   *
   * The "Instruction" struct can be used to access every instruction in a
   * generic way.
   *
   * It can be accessed by index (instruction index) oder a direct pointer
   * to a instruction slot (IP).
   */
  std::vector<instr_slot_t> bytecode_;

  /**
   * Call contexts that belong to ExternalCallInstructions in the bytecode,
   * accessed by index.
   */
  std::vector<ExternalCallContext> external_call_contexts_;

  /**
   * Hierarchical array of further bytecode functions belonging to
   * InteralFunctionCalls, accessed by index.
   */
  std::vector<BytecodeFunction> sub_functions_;

  /**
   * Constant map created at system startup, that maps the function name string
   * of explicit defined function to their opcode. This way the function name
   * lookup is implicitly made with a hash table.
   */
  static const std::unordered_map<std::string, Opcode>
      explicit_call_opcode_mapping_;

#ifndef NDEBUG
  /**
   * In Debug mode: Maps every bytecode instruction slot to the
   * LLVM instruction it was created from.
   */
  std::vector<const llvm::Instruction *> instruction_trace_;
#endif

 private:
  friend BytecodeInterpreter;
  friend BytecodeBuilder;
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
