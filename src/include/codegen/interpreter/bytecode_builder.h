//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_builder.h
//
// Identification: src/include/codegen/interpreter/bytecode_builder.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <ffi.h>
#include <llvm/ADT/PostOrderIterator.h>
#include <llvm/IR/CFG.h>
#include <cmath>
#include <cstdint>
#include <memory>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "codegen/interpreter/bytecode_function.h"

namespace llvm {
class Instruction;
class Function;
class Value;
class BasicBlock;
class Type;
class Constant;
class CallInst;
class ExtractValueInst;
}  // namespace llvm

namespace peloton {
namespace codegen {

class CodeContext;

namespace interpreter {

class BytecodeBuilder {
 public:
  /**
   * Static method to create a bytecode function from a code context.
   * @param code_context CodeContext containing the LLVM function
   * @param function LLVM function that shall be interpreted later
   * @return A BytecodeFunction object that can be passed to the
   * BytecodeInterpreter (several times).
   */
  static BytecodeFunction CreateBytecodeFunction(
      const CodeContext &code_context, const llvm::Function *function,
      bool use_naive_register_allocator = false);

 private:
  // These types definitions have the purpose to make the code better
  // understandable. The bytecode builder creates indexes to identify the
  // LLVM types, which usually are only accessed by raw pointers.
  // Those types shall indicate which index is meant by a function.
  // None of these indexes end up in the bytecode function!
  using value_index_t = index_t;
  using instruction_index_t = index_t;

  /**
   * Describes a bytecode relocation that has to be applied to add the
   * destination of a branch instruction once its value is available.
   * It gets created by TranslateBranch() and is processed after
   * TranslateFunction() processed all instructions.
   */
  struct BytecodeRelocation {
    index_t instruction_slot;
    index_t argument;
    const llvm::BasicBlock *bb;
  };

  /**
   * Describes the liveness of a value by start and end instruction index.
   */
  using ValueLiveness = std::pair<index_t, index_t>;

 private:
  BytecodeBuilder(const CodeContext &code_context,
                  const llvm::Function *function);

  /**
   * Analyses the function to collect values and constants and gets
   * value liveness information
   */
  void AnalyseFunction();

  /**
   * Naive register allocation that just assings a unique value slot to
   * every value
   */
  void PerformNaiveRegisterAllocation();

  /**
   * Greedy register allocation, that for each value tries to find the next free
   * value slot, that is not occupied anymore.
   */
  void PerformGreedyRegisterAllocation();

  /**
   * Translates all instructions into bytecode.
   */
  void TranslateFunction();

  /**
   * Do some final conversations to make the created BytecodeFunction usable.
   */
  void Finalize();

 private:
  //===--------------------------------------------------------------------===//
  // Methods for Value Handling
  //===--------------------------------------------------------------------===//

  /**
   * Gets the value index for a given LLVM value. If no value index exists
   * for this LLVM value, a new one is created.
   * @param value LLVM Value
   * @return the value index that is mapped to this LLVM value
   */
  value_index_t GetValueIndex(const llvm::Value *value);

  /**
   * Maps a given LLVM value to the same value index as another LLVM Value.
   * @param alias LLVM value
   * @param value_index the value index to map the LLVM value to. The
   * value index must already exist.
   * @return the value index that was given as parameter
   */
  value_index_t CreateValueAlias(const llvm::Value *alias,
                                 value_index_t value_index);

  /**
   * Returns the value_index for a LLVM constant.
   * In LLVM several Constant Objects with the same value can exist. This
   * function tries to find an existing constant with the same value or creates
   * a new one if necessary.
   * @param constant LLVM constant
   * @return a value index that refers to a constant with the same value. If
   * no internal constant with this value exists before, a new value index
   * is created.
   */
  value_index_t GetConstantIndex(const llvm::Constant *constant);

  /**
   * Returns the value slot (register) for a given LLVM value
   * @param value LLVM value
   * @return the value slot (register) assigned by the register allocation
   * This function must not be called before the Analysis pass and the
   * Register Allocation!
   */
  index_t GetValueSlot(const llvm::Value *value) const;

  /**
   * Extends the liveness range of a value to cover the given instruction index.
   * The will be extended to the "left" or the "right" if necessary, or not at
   * all, if it already covers this index.
   * This function calls GetValueIndex, which may create a new value index.
   * @param llvm_value LLVM value for which the liveness should be extended
   * @param instruction_index position in the
   */
  void ExtendValueLiveness(const llvm::Value *llvm_value,
                           instruction_index_t instruction_index);

  /**
   * Returns the index for a additional temporary value slot in that
   * basic block. Due to the phi swap problem (lost copy) it can happen,
   * that during translation additional value slots are needed that have not
   * been mapped by the register allocation. The number of additional temporary
   * value slots is tracked and added to the overall number of value
   * slots during finalization.
   * @param bb basic block the temporary value slot shall be created in
   * @return a temporary value slot index, that can be used only in
   * this basic block
   */
  index_t GetTemporaryValueSlot(const llvm::BasicBlock *bb);

  //===--------------------------------------------------------------------===//
  // Helper Functions (const)
  //===--------------------------------------------------------------------===//

  /**
   * Returns the matching FFI type for a given LLVM type
   * @param type LLVM type
   * @return FFI type
   */
  ffi_type *GetFFIType(llvm::Type *type) const;

  /**
   * Checks if a LLVM Value is a constant
   * @param value LLVM Value
   * @return true, if the given LLVM value is a constant
   */
  bool IsConstantValue(const llvm::Value *value) const;

  /**
   * Extracts the actual constant value of a LLVM constant
   * @param constant LLVM constant
   * @return the actual value of the constant, sign or zero extended to
   * the size of value_t
   */
  value_t GetConstantValue(const llvm::Constant *constant) const;

  /**
   * Directly extracts the signed integer value of a integer constant
   * @param constant LLVM Constant that is a instance of llvm::ConstantInt
   * @return signed integer value of the LLVM constant
   */
  int64_t GetConstantIntegerValueSigned(llvm::Value *constant) const;

  /**
   * Directly extracts the unsigned integer value of a integer constant
   * @param constant LLVM Constant that is a instance of llvm::ConstantInt
   * @return unsigned integer value of the LLVM constant
   */
  uint64_t GetConstantIntegerValueUnsigned(llvm::Value *constant) const;

  /**
   * Checks if one basic block is the successor of another basic block
   * when walking all basic blocks in reverse post order.
   * (Because ->nextNode doesn't work then)
   * @param bb current LLVM basic block
   * @param succ LLVM basic block that shall be checked to be the successor
   * @return true, if succ is the successor of bb
   */
  bool BasicBlockIsRPOSucc(const llvm::BasicBlock *bb,
                           const llvm::BasicBlock *succ) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined for
   * _all_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_ALL_TYPES(op), where op must be defined for all types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeAllTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _integer_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_INT_TYPES(op), where op must be defined only for integer types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeIntTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _floating point_ types
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_FLOAT_TYPES(op), where op must be defined only for float types.
   * @param type LLVM type to take the type information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeFloatTypes(Opcode untyped_op, llvm::Type *type) const;

  /**
   * Creates the typed opcode for a bytecode instruction that is defined only
   * for _integer_ types. In difference to the other function, this one only
   * considers the type size to determine the opcode type.
   * @param untyped_op untyped opcode for a byte instruction, retrieved using
   * GET_FIRST_INT_TYPES(op), where op must be defined only for integer types.
   * @param type LLVM type to take the size information from
   * @return typed opcode <op>_<type>
   */
  Opcode GetOpcodeForTypeSizeIntTypes(Opcode untyped_op,
                                      llvm::Type *type) const;

  //===--------------------------------------------------------------------===//
  // Methods for creating Bytecode Instructions
  //===--------------------------------------------------------------------===//

  /**
   * Insert a bytecode instruction into the bytecode stream.
   */
  Instruction &InsertBytecodeInstruction(
      const llvm::Instruction *llvm_instruction, Opcode opcode,
      const std::vector<index_t> &args);

  /**
   * Insert a bytecode instruction into the bytecode stream.
   * Wrapper that automatically gets the value slots for the LLVM values
   * provided.
   */
  Instruction &InsertBytecodeInstruction(
      const llvm::Instruction *llvm_instruction, Opcode opcode,
      const std::vector<const llvm::Value *> &args);

  /**
   * Insert a external call bytecode instruction into the bytecode stream.
   * @param llvm_instruction LLVM function this instruction is created from.
   * (Only needed for tracing information, not used in Release mode!)
   * @param call_context index of the call context created for this external
   * call instruction
   * @param function function pointer to the external function
   * @return Reference to the created instruction in the bytecode stream.
   */
  ExternalCallInstruction &InsertBytecodeExternalCallInstruction(
      const llvm::Instruction *llvm_instruction, index_t call_context,
      void *function);

  /**
   * Insert a internal call bytecode instruction into the bytecode stream.
   * @param llvm_instruction LLVM function this instruction is created from.
   * (Only needed for tracing information, not used in Release mode!)
   * @param sub_function index to the sub function (bytecode function) for
   * this LLVM function
   * @param dest_slot Destination slot for the return value. Set zero if
   * internal function returns void.
   * @param number_arguments number of arguments provided in this function call.
   * The internal call instruction has variadic size, depending on the number
   * of arguments!
   * @return Reference to the created instruction in the bytecode stream.
   */
  InternalCallInstruction &InsertBytecodeInternalCallInstruction(
      const llvm::Instruction *llvm_instruction, index_t sub_function,
      index_t dest_slot, size_t number_arguments);

/**
 * Helper function, that adds the given instruction to the instruction trace.
 * (Should only be called from InsertBytecode instructions)
 * In Release mode this function compiles to a stub.
 * @param llvm_instruction LLVM instruction the just created bytecode
 * instruction originates from
 * @param number_instruction_slots size of the bytecode instruction
 */
#ifndef NDEBUG
  void AddInstructionToTrace(const llvm::Instruction *llvm_instruction,
                             size_t number_instruction_slots = 1);
#else
  void AddInstructionToTrace(
      UNUSED_ATTRIBUTE const llvm::Instruction *llvm_instruction,
      UNUSED_ATTRIBUTE size_t number_instruction_slots = 1) {}
#endif

  //===--------------------------------------------------------------------===//
  // Methods for Translating LLVM Instructions (called by TranslateFunction())
  //===--------------------------------------------------------------------===//

  /**
   * Resolves the PHI nodes referring to this basic block, by placing mov
   * instructions. Must be called just before the terminating LLVM instruction
   * in a basic block. If the PHI swap / lost copy problem can occur, the
   * function creates additional mov instructions and value slots.
   * @param bb current basic block
   */
  void ProcessPHIsForBasicBlock(const llvm::BasicBlock *bb);

  void TranslateBranch(const llvm::Instruction *instruction,
                       std::vector<BytecodeRelocation> &bytecode_relocations);
  void TranslateReturn(const llvm::Instruction *instruction);
  void TranslateBinaryOperator(const llvm::Instruction *instruction);
  void TranslateAlloca(const llvm::Instruction *instruction);
  void TranslateLoad(const llvm::Instruction *instruction);
  void TranslateStore(const llvm::Instruction *instruction);
  void TranslateGetElementPtr(const llvm::Instruction *instruction);
  void TranslateIntExt(const llvm::Instruction *instruction);
  void TranslateFloatTruncExt(const llvm::Instruction *instruction);
  void TranslateFloatIntCast(const llvm::Instruction *instruction);
  void TranslateCmp(const llvm::Instruction *instruction);
  void TranslateCall(const llvm::Instruction *instruction);
  void TranslateSelect(const llvm::Instruction *instruction);
  void TranslateExtractValue(const llvm::Instruction *instruction);

 private:
  /**
   *  The bytecode function that is created (and then moved). All other
   * members are helping data structures that don't end up in the resulting
   * bytecode function
   */
  BytecodeFunction bytecode_function_;

  /**
   * Mapping from Value* to internal value index (includes merged
   * values/constants). The value index is used to access the vectors below.
   */
  std::unordered_map<const llvm::Value *, value_index_t> value_mapping_;

  /**
   * Holds the value liveness per value (after analysis)
   */
  std::vector<ValueLiveness> value_liveness_;

  /**
   * Holds the assigned value slot per value (after register allocation)
   */
  std::vector<index_t> value_slots_;

  /**
   * Overall number of value slots needed (from register allocation)
   * without temporary value slots (added during translation)
   */
  size_t number_value_slots_;

  /**
   * Holds the value_index of the constants in bytecode_function_.constants_,
   * accessed with the same index.
   */
  std::vector<value_index_t> constant_value_indexes_;

  /**
   * Additional temporary value slots (created due to phi swap problem).
   * Mapping from instruction index to number of temporary slots needed
   * at that time (specified by instruction index).
   */
  std::unordered_map<const llvm::BasicBlock *, index_t>
      number_temporary_values_;

  /**
   * Maximum number of temporary value slots needed at all time points.
   */
  size_t number_temporary_value_slots_;

  /**
   * Keep track of all Call instructions that refer to a overflow aware
   * operation, as their results get directly saved in the destination slots
   * of the ExtractValue instructions refering to them.
   */
  std::unordered_map<
      const llvm::CallInst *,
      std::pair<const llvm::ExtractValueInst *, const llvm::ExtractValueInst *>>
      overflow_results_mapping_;

  /**
   * Mapping of LLVM functions to bytecode functions to avoid duplicated
   * functions in case a internal function is called several times
   */
  std::unordered_map<const llvm::Function *, index_t> sub_function_mapping_;

  /**
   * ReversePostOrderTraversal, which is used for all BB traversals
   * Initialization is very expensive, so we reuse it
   * cannot be const, because the class doesn't provide const iterators
   */
  llvm::ReversePostOrderTraversal<const llvm::Function *> rpo_traversal_;

  /**
   * A vector holding the the basic block pointers in reverse post order.
   * This vector is retrieved from the RPO traversal and necessary
   * to make pred/pred lookups.
   */
  std::vector<const llvm::BasicBlock *> bb_reverse_post_order_;

  /**
   * Original code context the bytecode function is build from
   */
  const CodeContext &code_context_;

  /**
   * LLVM function that shall be translated
   */
  const llvm::Function *llvm_function_;
};

class NotSupportedException : public std::runtime_error {
 public:
  NotSupportedException(std::string message) : std::runtime_error(message) {}
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
