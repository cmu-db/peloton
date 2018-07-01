//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_builder.cpp
//
// Identification: src/codegen/interpreter/bytecode_builder.cpp
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/interpreter/bytecode_builder.h"

#include <llvm/IR/InstIterator.h>
#include <fstream>

#include "codegen/codegen.h"
#include "common/exception.h"
#include "util/math_util.h"

namespace peloton {
namespace codegen {
namespace interpreter {

BytecodeBuilder::BytecodeBuilder(const CodeContext &code_context,
                                 const llvm::Function *function)
    : bytecode_function_(function->getName().str()),
      number_value_slots_(0),
      number_temporary_value_slots_(0),
      rpo_traversal_(function),
      code_context_(code_context),
      llvm_function_(function) {}

BytecodeFunction BytecodeBuilder::CreateBytecodeFunction(
    const CodeContext &code_context, const llvm::Function *function,
    bool use_naive_register_allocator) {
  BytecodeBuilder builder(code_context, function);
  builder.AnalyseFunction();

  if (use_naive_register_allocator) {
    builder.PerformNaiveRegisterAllocation();
  } else {
    builder.PerformGreedyRegisterAllocation();
  }

  builder.TranslateFunction();
  builder.Finalize();

  return std::move(builder.bytecode_function_);
}

Opcode BytecodeBuilder::GetOpcodeForTypeAllTypes(Opcode untyped_op,
                                                 llvm::Type *type) const {
  index_t id = BytecodeFunction::GetOpcodeId(untyped_op);

  // This function highly depends on the macros in bytecode_instructions.def!

  if (type == code_context_.bool_type_ || type == code_context_.int8_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 0);
  } else if (type == code_context_.int16_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 1);
  } else if (type == code_context_.int32_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 2);
  } else if (type == code_context_.int64_type_ ||
             type == code_context_.char_ptr_type_ || type->isPointerTy()) {
    return BytecodeFunction::GetOpcodeFromId(id + 3);
  } else if (type == code_context_.float_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 4);
  } else if (type == code_context_.double_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 5);
  } else {
    throw NotSupportedException("llvm type not supported: " +
                                CodeGen::Dump(type));
  }
}

Opcode BytecodeBuilder::GetOpcodeForTypeIntTypes(Opcode untyped_op,
                                                 llvm::Type *type) const {
  index_t id = BytecodeFunction::GetOpcodeId(untyped_op);

  // This function highly depends on the macros in bytecode_instructions.def!

  if (type == code_context_.bool_type_ || type == code_context_.int8_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 0);
  } else if (type == code_context_.int16_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 1);
  } else if (type == code_context_.int32_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 2);
  } else if (type == code_context_.int64_type_ ||
             type == code_context_.char_ptr_type_ || type->isPointerTy()) {
    return BytecodeFunction::GetOpcodeFromId(id + 3);
  } else {
    throw NotSupportedException("llvm type not supported: " +
                                CodeGen::Dump(type));
  }
}

Opcode BytecodeBuilder::GetOpcodeForTypeFloatTypes(Opcode untyped_op,
                                                   llvm::Type *type) const {
  index_t id = BytecodeFunction::GetOpcodeId(untyped_op);

  // This function highly depends on the macros in bytecode_instructions.def!

  // float is missing!
  if (type == code_context_.float_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 0);
  } else if (type == code_context_.double_type_) {
    return BytecodeFunction::GetOpcodeFromId(id + 1);
  } else {
    throw NotSupportedException("llvm type not supported: " +
                                CodeGen::Dump(type));
  }
}

Opcode BytecodeBuilder::GetOpcodeForTypeSizeIntTypes(Opcode untyped_op,
                                                     llvm::Type *type) const {
  index_t id = BytecodeFunction::GetOpcodeId(untyped_op);

  // This function highly depends on the macros in bytecode_instructions.def!

  switch (code_context_.GetTypeSize(type)) {
    case 1:
      return BytecodeFunction::GetOpcodeFromId(id + 0);

    case 2:
      return BytecodeFunction::GetOpcodeFromId(id + 1);

    case 4:
      return BytecodeFunction::GetOpcodeFromId(id + 2);

    case 8:
      return BytecodeFunction::GetOpcodeFromId(id + 3);

    default:
      throw NotSupportedException("llvm type size not supported: " +
                                  CodeGen::Dump(type));
  }
}

Instruction &BytecodeBuilder::InsertBytecodeInstruction(
    const llvm::Instruction *llvm_instruction, Opcode opcode,
    const std::vector<index_t> &args) {
  PELOTON_ASSERT(opcode != Opcode::undefined);

  // calculate number of required instruction slots
  // args.size() + 1 because of the Opcode
  const size_t number_instruction_slots = MathUtil::DivRoundUp(
      sizeof(uint16_t) * (1 + args.size()), sizeof(instr_slot_t));

  bytecode_function_.bytecode_.insert(bytecode_function_.bytecode_.end(),
                                      number_instruction_slots, 0);
  Instruction &instruction = *reinterpret_cast<Instruction *>(
      &*(bytecode_function_.bytecode_.end() - number_instruction_slots));
  instruction.op = opcode;
  for (size_t i = 0; i < args.size(); i++) instruction.args[i] = args[i];

  AddInstructionToTrace(llvm_instruction, number_instruction_slots);

  return instruction;
}

Instruction &BytecodeBuilder::InsertBytecodeInstruction(
    const llvm::Instruction *llvm_instruction, Opcode opcode,
    const std::vector<const llvm::Value *> &args) {
  PELOTON_ASSERT(opcode != Opcode::undefined);

  std::vector<index_t> args_transformed(args.size());
  std::transform(
      args.begin(), args.end(), args_transformed.begin(),
      [this](const llvm::Value *value) { return GetValueSlot(value); });

  return InsertBytecodeInstruction(llvm_instruction, opcode, args_transformed);
}

ExternalCallInstruction &BytecodeBuilder::InsertBytecodeExternalCallInstruction(
    const llvm::Instruction *llvm_instruction, index_t call_context,
    void *function) {
  // calculate number of required instructionsslots and assert it is 2
  // (this way we recognise if any unintended size changes)
  const size_t number_instruction_slots = MathUtil::DivRoundUp(
      sizeof(ExternalCallInstruction), sizeof(instr_slot_t));
  PELOTON_ASSERT(number_instruction_slots == 2);

  bytecode_function_.bytecode_.insert(bytecode_function_.bytecode_.end(),
                                      number_instruction_slots, 0);

  ExternalCallInstruction instruction = {
      Opcode::call_external, call_context,
      reinterpret_cast<void (*)(void)>(function)};

  instr_slot_t *instruction_slot =
      &*(bytecode_function_.bytecode_.end() - number_instruction_slots);
  ExternalCallInstruction *call_instruction_slot =
      reinterpret_cast<ExternalCallInstruction *>(instruction_slot);
  *call_instruction_slot = instruction;

  AddInstructionToTrace(llvm_instruction, number_instruction_slots);

  return reinterpret_cast<ExternalCallInstruction &>(
      bytecode_function_.bytecode_[bytecode_function_.bytecode_.size() -
                                   number_instruction_slots]);
}

InternalCallInstruction &BytecodeBuilder::InsertBytecodeInternalCallInstruction(
    const llvm::Instruction *llvm_instruction, index_t sub_function,
    index_t dest_slot, size_t number_arguments) {
  // calculate number of required instruction slots
  // number_arguments + 4 because of the number of fixed arguments
  // (see structure of InternalCallInstruction)
  const size_t number_instruction_slots = MathUtil::DivRoundUp(
      sizeof(uint16_t) * (4 + number_arguments), sizeof(instr_slot_t));

  bytecode_function_.bytecode_.insert(bytecode_function_.bytecode_.end(),
                                      number_instruction_slots, 0);
  InternalCallInstruction &instruction =
      *reinterpret_cast<InternalCallInstruction *>(
          &*(bytecode_function_.bytecode_.end() - number_instruction_slots));
  instruction.op = Opcode::call_internal;
  instruction.sub_function = sub_function;
  instruction.dest_slot = dest_slot;
  instruction.number_args = static_cast<index_t>(number_arguments);

  PELOTON_ASSERT(
      &instruction.args[number_arguments - 1] <
      reinterpret_cast<index_t *>(&bytecode_function_.bytecode_.back() + 1));

  AddInstructionToTrace(llvm_instruction, number_instruction_slots);

  return reinterpret_cast<InternalCallInstruction &>(
      *(bytecode_function_.bytecode_.end() - number_instruction_slots));
}

#ifndef NDEBUG
void BytecodeBuilder::AddInstructionToTrace(
    const llvm::Instruction *llvm_instruction,
    size_t number_instruction_slots) {
  bytecode_function_.instruction_trace_.insert(
      bytecode_function_.instruction_trace_.end(), number_instruction_slots,
      llvm_instruction);
}
#endif

BytecodeBuilder::value_index_t BytecodeBuilder::GetValueIndex(
    const llvm::Value *value) {
  auto result = value_mapping_.find(value);

  // If the index already exists, just return it
  if (result != value_mapping_.end()) {
    return result->second;
  }

  // Otherwise create a new index

  // Special case for constants
  if (auto *llvm_constant = llvm::dyn_cast<llvm::Constant>(value)) {
    return GetConstantIndex(llvm_constant);
  }

  value_index_t value_index = value_liveness_.size();
  value_mapping_[value] = value_index;
  value_liveness_.emplace_back(std::numeric_limits<index_t>::max(),
                               std::numeric_limits<index_t>::max());
  return value_index;
}

BytecodeBuilder::value_index_t BytecodeBuilder::CreateValueAlias(
    const llvm::Value *alias, value_index_t value_index) {
  PELOTON_ASSERT(value_mapping_.find(alias) == value_mapping_.end());
  value_mapping_[alias] = value_index;

  return value_index;
}

value_t BytecodeBuilder::GetConstantValue(
    const llvm::Constant *constant) const {
  llvm::Type *type = constant->getType();

  if (constant->isNullValue() || constant->isZeroValue() || llvm::isa<llvm::UndefValue>(constant)) {
    return 0;
  } else {
    switch (type->getTypeID()) {
      case llvm::Type::IntegerTyID: {
        int64_t value_signed =
            llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
        return *reinterpret_cast<value_t *>(&value_signed);
      }

      case llvm::Type::FloatTyID: {
        float value_float = llvm::cast<llvm::ConstantFP>(constant)
                                ->getValueAPF()
                                .convertToFloat();
        return *reinterpret_cast<value_t *>(&value_float);
      }

      case llvm::Type::DoubleTyID: {
        double value_double = llvm::cast<llvm::ConstantFP>(constant)
                                  ->getValueAPF()
                                  .convertToDouble();

        return *reinterpret_cast<value_t *>(&value_double);
      }

      case llvm::Type::PointerTyID: {
        if (constant->getNumOperands() > 0) {
          if (auto *constant_int =
                  llvm::dyn_cast<llvm::ConstantInt>(constant->getOperand(0))) {
            return reinterpret_cast<value_t>(constant_int->getZExtValue());
          }
        }

        PELOTON_FALLTHROUGH;
      }

      default:
        throw NotSupportedException("unsupported constant type: " +
                                    CodeGen::Dump(constant->getType()));
    }
  }
}

BytecodeBuilder::value_index_t BytecodeBuilder::GetConstantIndex(
    const llvm::Constant *constant) {
  auto value_mapping_result = value_mapping_.find(constant);
  if (value_mapping_result != value_mapping_.end()) {
    return value_mapping_result->second;
  }

  value_t value = GetConstantValue(constant);
  value_index_t value_index;

  // We merge all constants that share the same value (not the type!)

  // Check if entry with this value already exists
  auto constant_result = std::find(bytecode_function_.constants_.begin(),
                                   bytecode_function_.constants_.end(), value);

  if (constant_result == bytecode_function_.constants_.end()) {
    // create new constant with that value
    value_index = value_liveness_.size();
    value_mapping_[constant] = value_index;
    value_liveness_.emplace_back(0, 0);  // constant liveness starts at 0

    bytecode_function_.constants_.push_back(value);
    constant_value_indexes_.push_back(value_index);

    // constants liveness starts at program start
    value_liveness_[value_index].first = 0;
  } else {
    // value already exists, create alias
    auto constant_index =
        constant_result - bytecode_function_.constants_.begin();
    value_index = constant_value_indexes_[constant_index];
    CreateValueAlias(constant, value_index);
  }

  return value_index;
};

index_t BytecodeBuilder::GetValueSlot(const llvm::Value *value) const {
  auto result = value_mapping_.find(value);
  PELOTON_ASSERT(result != value_mapping_.end());

  return value_slots_[result->second];
}

void BytecodeBuilder::ExtendValueLiveness(
    const llvm::Value *llvm_value, instruction_index_t instruction_index) {
  value_index_t value_index = GetValueIndex(llvm_value);

  // Special case if no liveness information is available yet
  if (value_liveness_[value_index].first ==
      std::numeric_limits<index_t>::max()) {
    value_liveness_[value_index].first = instruction_index;
    value_liveness_[value_index].second = instruction_index;
    return;
  }

  if (instruction_index < value_liveness_[value_index].first) {
    value_liveness_[value_index].first = instruction_index;
  } else if (instruction_index > value_liveness_[value_index].second) {
    value_liveness_[value_index].second = instruction_index;
  }
}

index_t BytecodeBuilder::GetTemporaryValueSlot(const llvm::BasicBlock *bb) {
  // we basically count the number of additional value slots that are
  // requested per basic block

  // new entry in map is created automatically if necessary
  number_temporary_values_[bb]++;

  number_temporary_value_slots_ =
      std::max(number_temporary_value_slots_,
               static_cast<size_t>(number_temporary_values_[bb]));
  return number_value_slots_ + number_temporary_values_[bb] - 1;
}

ffi_type *BytecodeBuilder::GetFFIType(llvm::Type *type) const {
  if (type->isVoidTy()) {
    return &ffi_type_void;
  } else if (type->isPointerTy()) {
    return &ffi_type_pointer;
  } else if (type == code_context_.double_type_) {
    return &ffi_type_double;
  }

  // exact type not necessary, only size is important
  switch (code_context_.GetTypeSize(type)) {
    case 1:
      return &ffi_type_uint8;

    case 2:
      return &ffi_type_uint16;

    case 4:
      return &ffi_type_uint32;

    case 8:
      return &ffi_type_uint64;

    default:
      throw NotSupportedException(
          std::string("can't find a ffi_type for type: ") +
          CodeGen::Dump(type));
  }
}

bool BytecodeBuilder::IsConstantValue(const llvm::Value *value) const {
  auto *constant = llvm::dyn_cast<llvm::Constant>(value);
  return (constant != nullptr);
}

int64_t BytecodeBuilder::GetConstantIntegerValueSigned(
    llvm::Value *constant) const {
  return llvm::cast<llvm::ConstantInt>(constant)->getSExtValue();
}

uint64_t BytecodeBuilder::GetConstantIntegerValueUnsigned(
    llvm::Value *constant) const {
  return llvm::cast<llvm::ConstantInt>(constant)->getZExtValue();
}

bool BytecodeBuilder::BasicBlockIsRPOSucc(const llvm::BasicBlock *bb,
                                          const llvm::BasicBlock *succ) const {
  // walk the vector where we saved the basic block pointers in R
  // reverse post order (RPO)
  for (size_t i = 0; i < bb_reverse_post_order_.size() - 1; i++) {
    if (bb_reverse_post_order_[i] == bb &&
        bb_reverse_post_order_[i + 1] == succ) {
      return true;
    }
  }

  return false;
}

void BytecodeBuilder::AnalyseFunction() {
  std::unordered_map<const llvm::BasicBlock *, std::pair<index_t, index_t>>
      bb_instruction_index_range;

  /* The analyse pass does:
   * - determine the liveness of all values
   * - merge values of instructions that translate to nop
   * - merge constants and create list of constants
   * - extract some additional information, e.g. for overflow aware operations
   */

  // Process function arguments
  for (auto &argument : llvm_function_->args()) {
    // DEF: function arguments are already defined at function start
    ExtendValueLiveness(&argument, 0);
  }

  instruction_index_t instruction_index = 0;
  for (llvm::ReversePostOrderTraversal<const llvm::Function *>::rpo_iterator
           traversal_iterator = rpo_traversal_.begin();
       traversal_iterator != rpo_traversal_.end(); ++traversal_iterator) {
    const llvm::BasicBlock *bb = *traversal_iterator;

    // Add this basic block to the rpo vector for pred/succ lookups
    bb_reverse_post_order_.push_back(bb);

    bb_instruction_index_range[bb].first = instruction_index;

    // Iterate all instructions to collect the liveness information
    // There are exceptions for several instructions,
    // which are labeled and explained below.
    for (llvm::BasicBlock::const_iterator instr_iterator = bb->begin();
         instr_iterator != bb->end(); ++instr_iterator, ++instruction_index) {
      const llvm::Instruction *instruction = &(*instr_iterator);

      bool is_non_zero_gep = false;
      if (instruction->getOpcode() == llvm::Instruction::GetElementPtr &&
          !llvm::cast<llvm::GetElementPtrInst>(instruction)
               ->hasAllZeroIndices()) {
        is_non_zero_gep = true;
      }

      // PHI-Handling:
      // We do not process the PHI instructions directly, but at the end of a
      // basic block, we process all PHI instructions of the successor blocks,
      // that refer to the currect basic block. This is the position where we
      // will insert the mov instructions when we resolve the PHIs later.

      // Skip PHI instructions
      if (instruction->getOpcode() == llvm::Instruction::PHI) {
        continue;
      }

      // If next instruction is a terminator instruction, process
      // PHIs of succeeding basic blocks first
      if (llvm::isa<llvm::TerminatorInst>(instruction)) {
        bool found_back_edge = false;

        // For all successor basic blocks
        for (auto succ_iterator = llvm::succ_begin(bb);
             succ_iterator != llvm::succ_end(bb); ++succ_iterator) {
          // Iterate phi instructions
          for (llvm::BasicBlock::const_iterator instr_iterator =
                   succ_iterator->begin();
               auto *phi_instruction =
                   llvm::dyn_cast<llvm::PHINode>(&*instr_iterator);
               ++instr_iterator) {
            // extend lifetime of phi value itself
            ExtendValueLiveness(phi_instruction, instruction_index);

            // extend lifetime of its operand
            llvm::Value *phi_operand =
                phi_instruction->getIncomingValueForBlock(bb);
            // Similar to Exception 3, we extend the lifetime by one, to ensure
            // the other phi operations do not overwrite the operand
            ExtendValueLiveness(phi_operand, instruction_index + 1);
          }  // end iterate phi instructions

          // We also use iterating the basic block successors to find
          // back edges. If we have seen a successor basic block before, it
          // must be a back edge.
          if (!found_back_edge) {
            auto instruction_index_range =
                bb_instruction_index_range.find(*succ_iterator);
            if (instruction_index_range != bb_instruction_index_range.end()) {
              index_t back_edge_instruction_index =
                  instruction_index_range->second.first;

              // For all values that are live at that time...
              for (auto &liveness : value_liveness_) {
                if (liveness.first < back_edge_instruction_index &&
                    liveness.second >= back_edge_instruction_index) {
                  // ...extend lifetime of this value to survive back edge
                  // instruction_index + 1 is the index of the last
                  // instruction in this basic block
                  liveness.second = instruction_index + 1;
                }
              }

              found_back_edge = true;
            }
          }
        }  // end iterate successor basic blocks

        instruction_index++;

        // fall through (continue with terminator instruction)
      }

      // Exception 1: Skip the ExtractValue instructions we already
      // processed in Exception 6
      if (instruction->getOpcode() == llvm::Instruction::ExtractValue) {
        auto *extractvalue_instruction =
            llvm::cast<llvm::ExtractValueInst>(instruction);

        // Check if this extract refers to a overflow call instruction
        auto result = overflow_results_mapping_.find(
            llvm::cast<llvm::CallInst>(instruction->getOperand(0)));
        if (result != overflow_results_mapping_.end() &&
            (result->second.first == extractvalue_instruction ||
             result->second.second == extractvalue_instruction)) {
          continue;
        }

        // fall through
      }

      // USE: Iterate operands of instruction and extend their liveness
      for (llvm::Instruction::const_op_iterator op_iterator =
               instruction->op_begin();
           op_iterator != instruction->op_end(); ++op_iterator) {
        llvm::Value *operand = op_iterator->get();

        // constant operands
        if (IsConstantValue(operand)) {
          // Exception 2: the called function in a CallInst is also a constant
          // but we want to skip this one
          auto *call_instruction = llvm::dyn_cast<llvm::CallInst>(instruction);
          if (call_instruction != nullptr &&
              call_instruction->getCalledFunction() == &*operand) {
            continue;
          }

          // Exception 3: constant operands from GEP and extractvalue are not
          // needed, as they get encoded in the instruction itself
          if (instruction->getOpcode() == llvm::Instruction::GetElementPtr ||
              instruction->getOpcode() == llvm::Instruction::ExtractValue) {
            continue;
          }

          // USE: extend liveness of constant value
          ExtendValueLiveness(operand, instruction_index);

          // Exception 4: We extend the lifetime of GEP operands of GEPs
          // that don't translate to nop, by one, to make sure that the operands
          // don't get overridden when we split the GEP into several
          // instructions.
        } else if (is_non_zero_gep) {
          ExtendValueLiveness(operand, instruction_index + 1);  // extended!

          // A BasicBlock may be a label operand, but we don't need to track
          // them
        } else if (!llvm::isa<llvm::BasicBlock>(operand)) {
          ExtendValueLiveness(operand, instruction_index);
        }
      }

      // Exception 5: For some instructions we know in advance that they will
      // produce a nop, so we merge their value and their operand here
      if (instruction->getOpcode() == llvm::Instruction::BitCast ||
          instruction->getOpcode() == llvm::Instruction::Trunc ||
          instruction->getOpcode() == llvm::Instruction::PtrToInt ||
          (instruction->getOpcode() == llvm::Instruction::GetElementPtr &&
           llvm::cast<llvm::GetElementPtrInst>(instruction)
               ->hasAllZeroIndices())) {
        // merge operand resulting value
        CreateValueAlias(instruction,
                         GetValueIndex(instruction->getOperand(0)));
        continue;
      }

      // Exception 6: Call instructions to any overflow aware operation
      // have to be tracked, because we save their results directly in
      // the destination slots of the ExtractValue instructions referring
      // to them.
      if (instruction->getOpcode() == llvm::Instruction::Call) {
        // Check if the call instruction calls a overflow aware operation
        // (unfortunately there is no better way to check this)
        auto *call_instruction = llvm::cast<llvm::CallInst>(instruction);
        llvm::Function *function = call_instruction->getCalledFunction();
        if (function->isDeclaration()) {
          std::string function_name = function->getName().str();

          if (function_name.size() >= 13 &&
              function_name.substr(10, 13) == "with.overflow") {
            // create entry for this call
            overflow_results_mapping_[call_instruction] =
                std::make_pair(nullptr, nullptr);

            // Find the first ExtractValue instruction referring to this call
            // instruction for result and overflow each and put it in the
            // value_liveness vector here. The liveness of those
            // instructions has to be extended to the definition of the call
            // instruction, and this way we ensure that the vector is sorted
            // by lifetime start index and we avoid sorting it later.
            for (auto *user : call_instruction->users()) {
              auto *extract_instruction =
                  llvm::cast<llvm::ExtractValueInst>(user);
              size_t extract_index = *extract_instruction->idx_begin();

              if (extract_index == 0) {
                PELOTON_ASSERT(
                    overflow_results_mapping_[call_instruction].first ==
                    nullptr);
                overflow_results_mapping_[call_instruction].first =
                    extract_instruction;

              } else if (extract_index == 1) {
                PELOTON_ASSERT(
                    overflow_results_mapping_[call_instruction].second ==
                    nullptr);
                overflow_results_mapping_[call_instruction].second =
                    extract_instruction;
              }

              ExtendValueLiveness(extract_instruction, instruction_index);
            }

            // Do not process the result of this instruction,
            // as this value (the overflow result struct) doesn't exist
            // later in the bytecode.

            continue;
          }
        }
      }

      // DEF: save the instruction index as the liveness starting point
      if (!instruction->getType()->isVoidTy()) {
        ExtendValueLiveness(instruction, instruction_index);
      }
    }

    bb_instruction_index_range[bb].second = instruction_index - 1;
  }
}

void BytecodeBuilder::PerformNaiveRegisterAllocation() {
  // assign a value slot to every liveness range in value_liveness_
  value_slots_.resize(value_liveness_.size(), 0);
  index_t reg = 0;

  // process constants
  for (auto &constant_value_index : constant_value_indexes_) {
    value_slots_[constant_value_index] = reg++ + 1;
  }

  // process function arguments
  for (auto &argument : llvm_function_->args()) {
    value_index_t argument_value_index = GetValueIndex(&argument);
    value_slots_[argument_value_index] = reg++ + 1;
  }

  // iterate over other entries, which are already sorted
  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    // skip values that are never used (get assigned to dummy slot)
    if (value_liveness_[i].first == value_liveness_[i].second) {
      continue;
    }

    // some values (constants, function arguments) are processed already
    if (value_slots_[i] == 0) {
      value_slots_[i] = reg++ + 1;  // + 1 because 0 is dummy slot
    }
  }

  number_value_slots_ = reg + 1;
}

void BytecodeBuilder::PerformGreedyRegisterAllocation() {
  // assign a value slot to every liveness range in value_liveness_

  value_slots_.resize(value_liveness_.size(), 0);
  std::vector<ValueLiveness> registers(constant_value_indexes_.size() +
                                       llvm_function_->arg_size());
  index_t reg = 0;

  auto findEmptyRegister = [&registers](ValueLiveness liveness) {
    for (index_t i = 0; i < registers.size(); ++i) {
      if (registers[i].second <= liveness.first) {
        registers[i] = liveness;
        return i;
      }
    }

    // no empty register found, create new one
    registers.push_back(liveness);
    return static_cast<index_t>(registers.size() - 1);
  };

  // process constants
  for (auto &constant_value_index : constant_value_indexes_) {
    registers[reg] = value_liveness_[constant_value_index];
    value_slots_[constant_value_index] =
        reg++ + 1;  // + 1 because 0 is dummy slot
  }

  // process function arguments
  for (auto &argument : llvm_function_->args()) {
    value_index_t argument_value_index = GetValueIndex(&argument);
    registers[reg] = value_liveness_[argument_value_index];
    value_slots_[argument_value_index] =
        reg++ + 1;  // + 1 because 0 is dummy slot
  }

  PELOTON_ASSERT(registers.size() == reg);

// The vector value_liveness_ is already sorted by lifetime start index
// except for the constant values, which are already processed

#ifndef NDEBUG
  // additional check in debug mode, to ensure that our assertion that the
  // vector is already sorted by lifetime start index (except zero) is correct
  instruction_index_t instruction_index = 1;

  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    if (value_liveness_[i].first != 0) {
      PELOTON_ASSERT(value_liveness_[i].first >= instruction_index);
      instruction_index = value_liveness_[i].first;
    }
  }
#endif

  // iterate over other entries, which are already sorted
  for (value_index_t i = 0; i < value_liveness_.size(); ++i) {
    // skip values that are never used
    if (value_liveness_[i].first == value_liveness_[i].second) {
      continue;
    }

    if (value_slots_[i] == 0) {
      value_slots_[i] = findEmptyRegister(value_liveness_[i]) +
                        1;  // + 1 because 0 is dummy slot
    }
  }

  number_value_slots_ = registers.size() + 1;  // + 1 because 0 is dummy slot
}

void BytecodeBuilder::TranslateFunction() {
  // Map every basic block an index in the resulting bytecode stream. This
  // is needed to perform the relocations in the branch instructions.
  std::unordered_map<const llvm::BasicBlock *, index_t> bb_mapping;

  // Collect all bytecode relocations that have to be performed after
  // translation, when the mapping information in bb_mapping is complete.
  std::vector<BytecodeRelocation> bytecode_relocations;

  // Iterate the basic blocks in reverse post order (RPO)
  // Linear scan register allocation requires RPO traversal
  // Initializing the RPO traversal is expensice, so we initialize it once
  // for the BytecodeBuilder object and reuse it.
  for (llvm::ReversePostOrderTraversal<const llvm::Function *>::rpo_iterator
           traversal_iterator = rpo_traversal_.begin();
       traversal_iterator != rpo_traversal_.end(); ++traversal_iterator) {
    const llvm::BasicBlock *bb = *traversal_iterator;

    // add basic block mapping
    bb_mapping[bb] = bytecode_function_.bytecode_.size();

    // Interate all instruction in the basic block
    for (llvm::BasicBlock::const_iterator instr_iterator = bb->begin();
         instr_iterator != bb->end(); ++instr_iterator) {
      const llvm::Instruction *instruction = &(*instr_iterator);

      // Dispatch to the respective translator function
      switch (instruction->getOpcode()) {
        // Terminators
        case llvm::Instruction::Br:
          ProcessPHIsForBasicBlock(bb);
          TranslateBranch(instruction, bytecode_relocations);
          break;

        case llvm::Instruction::Ret:
          ProcessPHIsForBasicBlock(bb);
          TranslateReturn(instruction);
          break;

        // Standard binary operators
        // Logical operators
        case llvm::Instruction::Add:
        case llvm::Instruction::Sub:
        case llvm::Instruction::Mul:
        case llvm::Instruction::UDiv:
        case llvm::Instruction::SDiv:
        case llvm::Instruction::URem:
        case llvm::Instruction::SRem:
        case llvm::Instruction::Shl:
        case llvm::Instruction::LShr:
        case llvm::Instruction::And:
        case llvm::Instruction::Or:
        case llvm::Instruction::Xor:
        case llvm::Instruction::AShr:
        case llvm::Instruction::FAdd:
        case llvm::Instruction::FSub:
        case llvm::Instruction::FMul:
        case llvm::Instruction::FDiv:
        case llvm::Instruction::FRem:
          TranslateBinaryOperator(instruction);
          break;

        // Memory instructions
        case llvm::Instruction::Load:
          TranslateLoad(instruction);
          break;

        case llvm::Instruction::Store:
          TranslateStore(instruction);
          break;

        case llvm::Instruction::Alloca:
          TranslateAlloca(instruction);
          break;

        case llvm::Instruction::GetElementPtr:
          TranslateGetElementPtr(instruction);
          break;

        // Cast instructions
        case llvm::Instruction::BitCast:
          // bit casts translate to nop
          // values got already merged in analysis pass
          break;

        case llvm::Instruction::SExt:
        case llvm::Instruction::ZExt:
        case llvm::Instruction::IntToPtr:
          TranslateIntExt(instruction);
          break;

        case llvm::Instruction::Trunc:
        case llvm::Instruction::PtrToInt:
          // trunc translates to nop
          // values got already merged in analysis pass
          break;
        case llvm::Instruction::FPExt:
        case llvm::Instruction::FPTrunc:
          TranslateFloatTruncExt(instruction);

        case llvm::Instruction::UIToFP:
        case llvm::Instruction::SIToFP:
        case llvm::Instruction::FPToUI:
        case llvm::Instruction::FPToSI:
          TranslateFloatIntCast(instruction);
          break;

        // Other instructions
        case llvm::Instruction::ICmp:
        case llvm::Instruction::FCmp:
          TranslateCmp(instruction);
          break;

        case llvm::Instruction::PHI:
          // PHIs are handled before every terminating instruction
          break;

        case llvm::Instruction::Call:
          TranslateCall(instruction);
          break;

        case llvm::Instruction::Select:
          TranslateSelect(instruction);
          break;

        case llvm::Instruction::ExtractValue:
          TranslateExtractValue(instruction);
          break;

        case llvm::Instruction::Unreachable:
          // nop
          break;

        // Instruction is not supported
        default: { throw NotSupportedException("instruction not supported"); }
      }
    }
  }

  // apply the relocations required by the placed branch instructions
  for (auto &relocation : bytecode_relocations) {
    reinterpret_cast<Instruction *>(
        &bytecode_function_.bytecode_[relocation.instruction_slot])
        ->args[relocation.argument] = bb_mapping[relocation.bb];
  }
}

void BytecodeBuilder::Finalize() {
  // calculate final number of value slots during runtime
  bytecode_function_.number_values_ =
      number_value_slots_ + number_temporary_value_slots_;

  // check if number values exceeds bit range (unrealistic)
  if (bytecode_function_.number_values_ >=
      std::numeric_limits<index_t>::max()) {
    throw NotSupportedException("number of values exceeds max number of bits");
  }

  // prepare arguments
  bytecode_function_.number_function_arguments_ = llvm_function_->arg_size();
}

void BytecodeBuilder::ProcessPHIsForBasicBlock(const llvm::BasicBlock *bb) {
  struct AdditionalMove {
    const llvm::Instruction *instruction;
    index_t dest;
    index_t src;
  };

  // Takes track of additional moves (du to PHI swap problem) that have to be
  // applied after all PHI nodes have been processed.
  std::vector<AdditionalMove> additional_moves;

  for (auto succ_iterator = llvm::succ_begin(bb);
       succ_iterator != llvm::succ_end(bb); ++succ_iterator) {
    // If the basic block is its own successor, we take risk to run into the PHI
    // swap problem (lost copy problem). To avoid this, we move the values in
    // temporary registers and move them to their destination after processing
    // all other PHI nodes.
    if (*succ_iterator == bb) {
      for (auto instruction_iterator = succ_iterator->begin();
           auto *phi_node =
               llvm::dyn_cast<llvm::PHINode>(&*instruction_iterator);
           ++instruction_iterator) {
        index_t temp_slot = GetTemporaryValueSlot(bb);

        InsertBytecodeInstruction(
            phi_node, Opcode::phi_mov,
            {temp_slot, GetValueSlot(phi_node->getIncomingValueForBlock(bb))});
        additional_moves.push_back(
            {phi_node, GetValueSlot(phi_node), temp_slot});
      }

      // Common case: create mov instruction to destination slot
    } else {
      for (auto instruction_iterator = succ_iterator->begin();
           auto *phi_node =
               llvm::dyn_cast<llvm::PHINode>(&*instruction_iterator);
           ++instruction_iterator) {
        if (GetValueSlot(phi_node) ==
            GetValueSlot(phi_node->getIncomingValueForBlock(bb))) {
          continue;
        }

        InsertBytecodeInstruction(
            phi_node, Opcode::phi_mov,
            {phi_node, phi_node->getIncomingValueForBlock(bb)});
      }
    }
  }

  // Place additional moves if needed
  for (auto &entry : additional_moves) {
    InsertBytecodeInstruction(entry.instruction, Opcode::phi_mov,
                              {entry.dest, entry.src});
  }
}

void BytecodeBuilder::TranslateBranch(
    const llvm::Instruction *instruction,
    std::vector<BytecodeRelocation> &bytecode_relocations) {
  auto *branch_instruction = llvm::cast<llvm::BranchInst>(&*instruction);

  // conditional branch
  if (branch_instruction->isConditional()) {
    // The first operand in the IR is the false branch, while the second one
    // is the true one (printed llvm assembly is the other way round).
    // To be consistent, we use the order of the memory representation
    // in our bytecode.

    // If false branch is next basic block, we can use a fall through branch
    if (BasicBlockIsRPOSucc(
            branch_instruction->getParent(),
            llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(1)))) {
      InsertBytecodeInstruction(
          instruction, Opcode::branch_cond_ft,
          std::vector<index_t>{GetValueSlot(branch_instruction->getOperand(0)),
                               0});

      BytecodeRelocation relocation_false{
          static_cast<index_t>(bytecode_function_.bytecode_.size() - 1), 1,
          llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(2))};

      // add relocation entry, to insert missing information of destination
      // later
      bytecode_relocations.push_back(relocation_false);

      // no fall through
    } else {
      InsertBytecodeInstruction(
          instruction, Opcode::branch_cond,
          {GetValueSlot(branch_instruction->getOperand(0)), 0, 0});

      BytecodeRelocation relocation_false{
          static_cast<index_t>(bytecode_function_.bytecode_.size() - 1), 1,
          llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(1))};

      // add relocation entry, to insert missing information of destination
      // later
      bytecode_relocations.push_back(relocation_false);

      BytecodeRelocation relocation_true{
          static_cast<index_t>(bytecode_function_.bytecode_.size() - 1), 2,
          llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(2))};

      // add relocation entry, to insert missing information of destination
      // later
      bytecode_relocations.push_back(relocation_true);
    }

    // unconditional branch
  } else {
    // If the unconditional branch points to the next basic block,
    // we can omit the branch instruction
    if (!BasicBlockIsRPOSucc(
            branch_instruction->getParent(),
            llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(0)))) {
      InsertBytecodeInstruction(instruction, Opcode::branch_uncond,
                                std::vector<index_t>{0});

      BytecodeRelocation relocation{
          static_cast<index_t>(bytecode_function_.bytecode_.size() - 1), 0,
          llvm::cast<llvm::BasicBlock>(branch_instruction->getOperand(0))};

      // add relocation entry, to insert missing information of destination
      // later
      bytecode_relocations.push_back(relocation);
    }
  }
}

void BytecodeBuilder::TranslateReturn(const llvm::Instruction *instruction) {
  auto *return_instruction = llvm::cast<llvm::ReturnInst>(&*instruction);

  // We only have one ret bytecode instruction. If the function returns void,
  // the instruction will return the value of the dummy value slot zero,
  // but no one will every pick up that value.

  index_t return_slot = 0;
  if (return_instruction->getNumOperands() > 0) {
    return_slot = GetValueSlot(return_instruction->getOperand(0));
  }

  InsertBytecodeInstruction(instruction, Opcode::ret,
                            std::vector<index_t>{return_slot});
}

void BytecodeBuilder::TranslateBinaryOperator(
    const llvm::Instruction *instruction) {
  auto *binary_operator = llvm::cast<llvm::BinaryOperator>(&*instruction);
  auto *type = binary_operator->getType();
  Opcode opcode;

  switch (binary_operator->getOpcode()) {
    case llvm::Instruction::Add:
    case llvm::Instruction::FAdd:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::add), type);
      break;

    case llvm::Instruction::Sub:
    case llvm::Instruction::FSub:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::sub), type);
      break;

    case llvm::Instruction::Mul:
    case llvm::Instruction::FMul:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::mul), type);
      break;

    case llvm::Instruction::UDiv:
    case llvm::Instruction::FDiv:
      opcode = GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::div), type);
      break;

    case llvm::Instruction::SDiv:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sdiv), type);
      break;

    case llvm::Instruction::URem:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::urem), type);
      break;

    case llvm::Instruction::FRem:
      opcode =
          GetOpcodeForTypeFloatTypes(GET_FIRST_FLOAT_TYPES(Opcode::frem), type);
      break;

    case llvm::Instruction::SRem:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::srem), type);
      break;

    case llvm::Instruction::Shl:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::shl), type);
      break;

    case llvm::Instruction::LShr:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::lshr), type);
      break;

    case llvm::Instruction::AShr:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::ashr), type);
      break;

    case llvm::Instruction::And:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::and), type);
      break;

    case llvm::Instruction::Or:
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode:: or), type);
      break;

    case llvm::Instruction::Xor:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode:: xor), type);
      break;

    default:
      throw NotSupportedException("binary operation not supported");
  }

  InsertBytecodeInstruction(instruction, opcode,
                            {binary_operator, binary_operator->getOperand(0),
                             binary_operator->getOperand(1)});
}

void BytecodeBuilder::TranslateAlloca(const llvm::Instruction *instruction) {
  auto *alloca_instruction = llvm::cast<llvm::AllocaInst>(&*instruction);
  Opcode opcode;

  // get type to allocate
  llvm::Type *type = alloca_instruction->getAllocatedType();

  // get type size in bytes
  size_t type_size = code_context_.GetTypeSize(type);

  if (alloca_instruction->isArrayAllocation()) {
    index_t array_size = GetValueSlot(alloca_instruction->getArraySize());
    opcode =
        GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::alloca_array),
                                 alloca_instruction->getArraySize()->getType());

    // type size is immediate value!
    InsertBytecodeInstruction(instruction, opcode,
                              {GetValueSlot(alloca_instruction),
                               static_cast<index_t>(type_size), array_size});
  } else {
    opcode = Opcode::alloca;
    // type size is immediate value!
    InsertBytecodeInstruction(
        instruction, opcode,
        {GetValueSlot(alloca_instruction), static_cast<index_t>(type_size)});
  }
}

void BytecodeBuilder::TranslateLoad(const llvm::Instruction *instruction) {
  auto *load_instruction = llvm::cast<llvm::LoadInst>(&*instruction);

  Opcode opcode = GetOpcodeForTypeSizeIntTypes(
      GET_FIRST_INT_TYPES(Opcode::load), load_instruction->getType());
  InsertBytecodeInstruction(
      instruction, opcode,
      {load_instruction, load_instruction->getPointerOperand()});
}

void BytecodeBuilder::TranslateStore(const llvm::Instruction *instruction) {
  auto *store_instruction = llvm::cast<llvm::StoreInst>(&*instruction);

  Opcode opcode =
      GetOpcodeForTypeSizeIntTypes(GET_FIRST_INT_TYPES(Opcode::store),
                                   store_instruction->getOperand(0)->getType());
  InsertBytecodeInstruction(
      instruction, opcode,
      std::vector<const llvm::Value *>{store_instruction->getPointerOperand(),
                                       store_instruction->getValueOperand()});
}

void BytecodeBuilder::TranslateGetElementPtr(
    const llvm::Instruction *instruction) {
  auto *gep_instruction = llvm::cast<llvm::GetElementPtrInst>(&*instruction);
  int64_t overall_offset = 0;

  // If the GEP translates to a nop, the values have been already merged
  // during the analysis pass
  if (gep_instruction->hasAllZeroIndices()) {
    return;
  }

  // The offset is an immediate constant, not a slot index
  // instruction is created here, but offset will be filled in later,
  // because we may merge it with constant array accesses
  auto &gep_offset_bytecode_instruction_ref = InsertBytecodeInstruction(
      gep_instruction, Opcode::gep_offset,
      {GetValueSlot(gep_instruction),
       GetValueSlot(gep_instruction->getPointerOperand()), 0});
  size_t gep_offset_bytecode_instruction_index =
      bytecode_function_.GetIndexFromIP(&gep_offset_bytecode_instruction_ref);

  // First index operand of the instruction is the array index for the
  // source type

  // Get type of struct/array which will be processed
  llvm::Type *type = gep_instruction->getSourceElementType();

  if (IsConstantValue(gep_instruction->getOperand(1))) {
    overall_offset +=
        code_context_.GetTypeSize(type) *
        GetConstantIntegerValueSigned(gep_instruction->getOperand(1));
  } else {
    index_t index = GetValueSlot(instruction->getOperand(1));
    Opcode opcode =
        GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::gep_array),
                                 instruction->getOperand(1)->getType());

    // size of array element is an immediate constant, not a slot index!
    InsertBytecodeInstruction(
        gep_instruction, opcode,
        {GetValueSlot(gep_instruction), index,
         static_cast<index_t>(code_context_.GetTypeSize(type))});
  }

  // Iterate remaining Indexes
  for (unsigned int operand_index = 2;
       operand_index < instruction->getNumOperands(); ++operand_index) {
    auto *operand = instruction->getOperand(operand_index);

    if (auto *array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
      if (IsConstantValue(operand)) {
        overall_offset +=
            code_context_.GetTypeSize(array_type->getElementType()) *
            GetConstantIntegerValueSigned(operand);
      } else {
        index_t index = GetValueSlot(operand);
        Opcode opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::gep_array), operand->getType());

        // size of array element is an immediate constant, not a slot index!
        InsertBytecodeInstruction(
            gep_instruction, opcode,
            {GetValueSlot(gep_instruction), index,
             static_cast<index_t>(
                 code_context_.GetTypeSize(array_type->getElementType()))});
      }

      // get inner type for next iteration
      type = array_type->getElementType();

    } else if (auto *struct_type = llvm::dyn_cast<llvm::StructType>(type)) {
      uint64_t index = GetConstantIntegerValueUnsigned(operand);
      PELOTON_ASSERT(index < struct_type->getNumElements());

      // get element offset
      overall_offset += code_context_.GetStructElementOffset(struct_type, index);

      // get inner type for next iteration
      type = struct_type->getElementType(index);

    } else {
      throw NotSupportedException(
          "unexpected type in getelementptr instruction");
    }
  }

  // make sure that resulting type is correct
  PELOTON_ASSERT(type == gep_instruction->getResultElementType());

  // fill in calculated overall offset in previously placed gep_offset
  // bytecode instruction
  // (use index instead of reference, as vector may has been relocated!)
  reinterpret_cast<Instruction *>(
      &bytecode_function_.bytecode_[gep_offset_bytecode_instruction_index])
      ->args[2] = static_cast<index_t>(overall_offset);
}

void BytecodeBuilder::TranslateFloatIntCast(
    const llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  // These instruction basically exist from every integer type to every
  // floating point type and the other way round.
  // We can only expand instructions in one dimension, so we expand the
  // integer dimension and create the floating point instances manually
  // (float and double)

  Opcode opcode = Opcode::undefined;

  if (instruction->getOpcode() == llvm::Instruction::FPToSI) {
    if (cast_instruction->getOperand(0)->getType() ==
        code_context_.float_type_) {
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::floattosi),
                                        cast_instruction->getType());
    } else if (cast_instruction->getOperand(0)->getType() ==
               code_context_.double_type_) {
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::doubletosi),
                                        cast_instruction->getType());
    } else {
      throw NotSupportedException("unsupported cast instruction");
    }

  } else if (instruction->getOpcode() == llvm::Instruction::FPToUI) {
    if (cast_instruction->getOperand(0)->getType() ==
        code_context_.float_type_) {
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::floattoui),
                                        cast_instruction->getType());
    } else if (cast_instruction->getOperand(0)->getType() ==
               code_context_.double_type_) {
      opcode = GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::doubletoui),
                                        cast_instruction->getType());
    } else {
      throw NotSupportedException("unsupported cast instruction");
    }

  } else if (instruction->getOpcode() == llvm::Instruction::SIToFP) {
    if (cast_instruction->getType() == code_context_.float_type_) {
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sitofloat),
                                   cast_instruction->getOperand(0)->getType());
    } else if (cast_instruction->getType() == code_context_.double_type_) {
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::sitodouble),
                                   cast_instruction->getOperand(0)->getType());
    } else {
      throw NotSupportedException("unsupported cast instruction");
    }

  } else if (instruction->getOpcode() == llvm::Instruction::UIToFP) {
    if (cast_instruction->getType() == code_context_.float_type_) {
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::uitofloat),
                                   cast_instruction->getOperand(0)->getType());
    } else if (cast_instruction->getType() == code_context_.double_type_) {
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::uitodouble),
                                   cast_instruction->getOperand(0)->getType());
    } else {
      throw NotSupportedException("unsupported cast instruction");
    }

  } else {
    throw NotSupportedException("unsupported cast instruction");
  }

  InsertBytecodeInstruction(
      cast_instruction, opcode,
      {cast_instruction, cast_instruction->getOperand(0)});
}

void BytecodeBuilder::TranslateIntExt(const llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  size_t src_type_size =
      code_context_.GetTypeSize(cast_instruction->getSrcTy());
  size_t dest_type_size =
      code_context_.GetTypeSize(cast_instruction->getDestTy());

  if (src_type_size == dest_type_size) {
    if (GetValueSlot(instruction) != GetValueSlot(instruction->getOperand(0)))
      InsertBytecodeInstruction(instruction, Opcode::nop_mov,
                                {instruction, instruction->getOperand(0)});
    return;
  }

  Opcode opcode = Opcode::undefined;

  if (instruction->getOpcode() == llvm::Instruction::SExt) {
    if (src_type_size == 1 && dest_type_size == 2) {
      opcode = Opcode::sext_i8_i16;

    } else if (src_type_size == 1 && dest_type_size == 4) {
      opcode = Opcode::sext_i8_i32;

    } else if (src_type_size == 1 && dest_type_size == 8) {
      opcode = Opcode::sext_i8_i64;

    } else if (src_type_size == 2 && dest_type_size == 4) {
      opcode = Opcode::sext_i16_i32;

    } else if (src_type_size == 2 && dest_type_size == 8) {
      opcode = Opcode::sext_i16_i64;

    } else if (src_type_size == 4 && dest_type_size == 8) {
      opcode = Opcode::sext_i32_i64;

    } else {
      throw NotSupportedException("unsupported sext instruction");
    }

  } else if (instruction->getOpcode() == llvm::Instruction::ZExt ||
             instruction->getOpcode() == llvm::Instruction::IntToPtr) {
    if (src_type_size == 1 && dest_type_size == 2) {
      opcode = Opcode::zext_i8_i16;

    } else if (src_type_size == 1 && dest_type_size == 4) {
      opcode = Opcode::zext_i8_i32;

    } else if (src_type_size == 1 && dest_type_size == 8) {
      opcode = Opcode::zext_i8_i64;

    } else if (src_type_size == 2 && dest_type_size == 4) {
      opcode = Opcode::zext_i16_i32;

    } else if (src_type_size == 2 && dest_type_size == 8) {
      opcode = Opcode::zext_i16_i64;

    } else if (src_type_size == 4 && dest_type_size == 8) {
      opcode = Opcode::zext_i32_i64;

    } else {
      throw NotSupportedException("unsupported zext instruction");
    }

  } else {
    throw NotSupportedException("unexpected ext instruction");
  }

  InsertBytecodeInstruction(
      cast_instruction, opcode,
      {cast_instruction, cast_instruction->getOperand(0)});
}

void BytecodeBuilder::TranslateFloatTruncExt(
    const llvm::Instruction *instruction) {
  auto *cast_instruction = llvm::dyn_cast<llvm::CastInst>(&*instruction);

  auto src_type = cast_instruction->getSrcTy();
  auto dest_type = cast_instruction->getDestTy();

  if (src_type == dest_type) {
    if (GetValueSlot(instruction) != GetValueSlot(instruction->getOperand(0))) {
      InsertBytecodeInstruction(instruction, Opcode::nop_mov,
                                {instruction, instruction->getOperand(0)});
    }
    return;
  }

  if (src_type == code_context_.double_type_ &&
      dest_type == code_context_.float_type_) {
    InsertBytecodeInstruction(
        cast_instruction, Opcode::doubletofloat,
        {cast_instruction, cast_instruction->getOperand(0)});
  } else if (src_type == code_context_.float_type_ &&
             dest_type == code_context_.double_type_) {
    InsertBytecodeInstruction(
        cast_instruction, Opcode::floattodouble,
        {cast_instruction, cast_instruction->getOperand(0)});
  } else {
    throw NotSupportedException("unsupported FPTrunc/PFExt instruction");
  }
}

void BytecodeBuilder::TranslateCmp(const llvm::Instruction *instruction) {
  auto *cmp_instruction = llvm::cast<llvm::CmpInst>(&*instruction);
  auto *type = cmp_instruction->getOperand(0)->getType();
  Opcode opcode = Opcode::undefined;

  switch (cmp_instruction->getPredicate()) {
    case llvm::CmpInst::Predicate::ICMP_EQ:
    case llvm::CmpInst::Predicate::FCMP_OEQ:
    case llvm::CmpInst::Predicate::FCMP_UEQ:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_eq), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_NE:
    case llvm::CmpInst::Predicate::FCMP_ONE:
    case llvm::CmpInst::Predicate::FCMP_UNE:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_ne), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGT:
    case llvm::CmpInst::Predicate::FCMP_OGT:
    case llvm::CmpInst::Predicate::FCMP_UGT:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_gt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_UGE:
    case llvm::CmpInst::Predicate::FCMP_OGE:
    case llvm::CmpInst::Predicate::FCMP_UGE:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_ge), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULT:
    case llvm::CmpInst::Predicate::FCMP_OLT:
    case llvm::CmpInst::Predicate::FCMP_ULT:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_lt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_ULE:
    case llvm::CmpInst::Predicate::FCMP_OLE:
    case llvm::CmpInst::Predicate::FCMP_ULE:
      opcode =
          GetOpcodeForTypeAllTypes(GET_FIRST_ALL_TYPES(Opcode::cmp_le), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SGT:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sgt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SGE:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sge), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SLT:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_slt), type);
      break;

    case llvm::CmpInst::Predicate::ICMP_SLE:
      opcode =
          GetOpcodeForTypeIntTypes(GET_FIRST_INT_TYPES(Opcode::cmp_sle), type);
      break;

    default:
      throw NotSupportedException("compare operand not supported");
  }

  InsertBytecodeInstruction(cmp_instruction, opcode,
                            {cmp_instruction, cmp_instruction->getOperand(0),
                             cmp_instruction->getOperand(1)});
}

void BytecodeBuilder::TranslateCall(const llvm::Instruction *instruction) {
  auto *call_instruction = llvm::cast<llvm::CallInst>(&*instruction);

  llvm::Function *function = call_instruction->getCalledFunction();

  if (function->isDeclaration()) {
    // The only way to find out about the called function (even if its an
    // intrinsic) is to check the function name string
    std::string function_name = function->getName().str();

    if (function_name.find("llvm.memcpy") == 0) {
      if (call_instruction->getOperand(2)->getType() !=
          code_context_.int64_type_) {
        throw NotSupportedException(
            "memcpy with different size type than i64 not supported");
      }

      InsertBytecodeInstruction(
          call_instruction, Opcode::llvm_memcpy,
          {call_instruction->getOperand(0), call_instruction->getOperand(1),
           call_instruction->getOperand(2)});

    } else if (function_name.find("llvm.memmove") == 0) {
      if (call_instruction->getOperand(2)->getType() !=
          code_context_.int64_type_)
        throw NotSupportedException(
            "memmove with different size type than i64 not supported");

      InsertBytecodeInstruction(
          call_instruction, Opcode::llvm_memmove,
          {call_instruction->getOperand(0), call_instruction->getOperand(1),
           call_instruction->getOperand(2)});

    } else if (function_name.find("llvm.memset") == 0) {
      if (call_instruction->getOperand(2)->getType() !=
          code_context_.int64_type_)
        throw NotSupportedException(
            "memset with different size type than i64 not supported");

      InsertBytecodeInstruction(
          call_instruction, Opcode::llvm_memset,
          {call_instruction->getOperand(0), call_instruction->getOperand(1),
           call_instruction->getOperand(2)});

    } else if (function_name.find("with.overflow") == 10) {
      index_t result = 0;
      index_t overflow = 0;
      auto *type = call_instruction->getOperand(0)->getType();
      Opcode opcode = Opcode::undefined;

      // The destination slots have been already prepared from the analysis pass
      PELOTON_ASSERT(overflow_results_mapping_.find(call_instruction) !=
                     overflow_results_mapping_.end());

      if (overflow_results_mapping_[call_instruction].first != nullptr) {
        result =
            GetValueSlot(overflow_results_mapping_[call_instruction].first);
      }

      if (overflow_results_mapping_[call_instruction].second != nullptr) {
        overflow =
            GetValueSlot(overflow_results_mapping_[call_instruction].second);
      }

      if (function_name.substr(5, 4) == "uadd") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_uadd_overflow), type);
      } else if (function_name.substr(5, 4) == "sadd") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_sadd_overflow), type);
      } else if (function_name.substr(5, 4) == "usub") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_usub_overflow), type);
      } else if (function_name.substr(5, 4) == "ssub") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_ssub_overflow), type);
      } else if (function_name.substr(5, 4) == "umul") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_umul_overflow), type);
      } else if (function_name.substr(5, 4) == "smul") {
        opcode = GetOpcodeForTypeIntTypes(
            GET_FIRST_INT_TYPES(Opcode::llvm_smul_overflow), type);
      } else {
        throw NotSupportedException(
            "the requested operation with overflow is not supported");
      }

      InsertBytecodeInstruction(
          call_instruction, opcode,
          {result, overflow, GetValueSlot(call_instruction->getOperand(0)),
           GetValueSlot(call_instruction->getOperand(1))});

    } else if (function_name.find("llvm.x86.sse42.crc32") == 0) {
      if (call_instruction->getType() != code_context_.int64_type_) {
        throw NotSupportedException(
            "sse42.crc32 with different size type than i64 not supported");
      }

      InsertBytecodeInstruction(
          call_instruction, Opcode::llvm_sse42_crc32,
          {call_instruction, call_instruction->getOperand(0),
           call_instruction->getOperand(1)});

    } else {
      Opcode opcode =
          BytecodeFunction::GetExplicitCallOpcodeByString(function_name);

      // call explicit instantiation of this function if available
      if (opcode != Opcode::undefined) {
        std::vector<const llvm::Value *> args;
        args.reserve(call_instruction->getNumArgOperands());

        if (!instruction->getType()->isVoidTy()) {
          args.push_back(call_instruction);
        }

        for (unsigned int i = 0; i < call_instruction->getNumArgOperands();
             i++) {
          args.push_back(call_instruction->getArgOperand(i));
        }

        InsertBytecodeInstruction(call_instruction, opcode, args);

      } else {
        // Function is not available in IR context, so we have to make an
        // external function call

        // lookup function pointer in code context
        void *raw_pointer = code_context_.LookupBuiltin(function_name).second;

        if (raw_pointer == nullptr) {
          throw NotSupportedException("could not find external function: " +
                                      function_name);
        }

        // libffi is used for external function calls
        // Here we collect all the information that will be needed at runtime
        // (function activation time) to create the libffi call interface.

        // Show a hint, that an explicit wrapper could be created for this
        // function
        LOG_DEBUG("The interpreter will call the C++ function '%s' per libffi. "
                  "Consider adding an explicit wrapper for this function in "
                  "bytecode_instructions.def\n", function_name.c_str());

        index_t dest_slot = 0;
        if (!instruction->getType()->isVoidTy()) {
          dest_slot = GetValueSlot(call_instruction);
        }

        size_t arguments_num = call_instruction->getNumArgOperands();
        ExternalCallContext call_context{
            dest_slot, GetFFIType(instruction->getType()),
            std::vector<index_t>(arguments_num),
            std::vector<ffi_type *>(arguments_num)};

        for (unsigned int i = 0; i < call_instruction->getNumArgOperands();
             i++) {
          call_context.args[i] =
              GetValueSlot(call_instruction->getArgOperand(i));
          call_context.arg_types[i] =
              GetFFIType(call_instruction->getArgOperand(i)->getType());
        }

        // add call context to bytecode function
        bytecode_function_.external_call_contexts_.push_back(call_context);

        // insert bytecode instruction referring to this call context
        InsertBytecodeExternalCallInstruction(
            call_instruction,
            static_cast<index_t>(
                bytecode_function_.external_call_contexts_.size() - 1),
            raw_pointer);
      }
    }
  } else {
    // Internal function call to another IR function in this code context

    index_t dest_slot = 0;
    if (!instruction->getType()->isVoidTy()) {
      dest_slot = GetValueSlot(call_instruction);
    }

    // Translate the bytecode function we want to call
    index_t sub_function_index;
    const auto result = sub_function_mapping_.find(function);
    if (result != sub_function_mapping_.end()) {
      sub_function_index = result->second;
    } else {
      auto sub_function =
          BytecodeBuilder::CreateBytecodeFunction(code_context_, function);

      bytecode_function_.sub_functions_.push_back(std::move(sub_function));
      sub_function_index = bytecode_function_.sub_functions_.size() - 1;
      sub_function_mapping_[function] = sub_function_index;
    }

    InternalCallInstruction &bytecode_instruction =
        InsertBytecodeInternalCallInstruction(
            call_instruction, sub_function_index, dest_slot,
            call_instruction->getNumArgOperands());

    for (unsigned int i = 0; i < call_instruction->getNumArgOperands(); i++) {
      bytecode_instruction.args[i] =
          GetValueSlot(call_instruction->getArgOperand(i));

      // just to make sure, we check that no function argument is bigger
      // than 8 Bytes
      if (code_context_.GetTypeSize(
              call_instruction->getArgOperand(i)->getType()) > 8) {
        throw NotSupportedException("argument for internal call too big");
      }
    }
  }
}

void BytecodeBuilder::TranslateSelect(const llvm::Instruction *instruction) {
  auto *select_instruction = llvm::cast<llvm::SelectInst>(&*instruction);

  InsertBytecodeInstruction(
      select_instruction, Opcode::select,
      {select_instruction, select_instruction->getCondition(),
       select_instruction->getTrueValue(),
       select_instruction->getFalseValue()});
}

void BytecodeBuilder::TranslateExtractValue(
    const llvm::Instruction *instruction) {
  auto *extract_instruction = llvm::cast<llvm::ExtractValueInst>(&*instruction);

  // Skip, if this ExtractValue instruction belongs to an overflow operation
  auto call_result = overflow_results_mapping_.find(
      llvm::cast<llvm::CallInst>(instruction->getOperand(0)));
  if (call_result != overflow_results_mapping_.end()) {
    return;
  }

  // Get value type
  llvm::Type *type = extract_instruction->getAggregateOperand()->getType();
  size_t offset_bits = 0;

  // make sure the result type fits in a value_t
  if (code_context_.GetTypeSize(instruction->getType()) <= sizeof(value_t)) {
    throw NotSupportedException("extracted value too big for register size");
  }

  // Iterate indexes
  for (auto index_it = extract_instruction->idx_begin(),
            index_end = extract_instruction->idx_end();
       index_it != index_end; index_it++) {
    uint32_t index = *index_it;

    if (auto *array_type = llvm::dyn_cast<llvm::ArrayType>(type)) {
      // Advance offset
      offset_bits +=
          code_context_.GetTypeAllocSizeInBits(array_type->getElementType()) *
          index;

      // get inner type for next iteration
      type = array_type->getElementType();
    } else if (auto *struct_type = llvm::dyn_cast<llvm::StructType>(type)) {
      PELOTON_ASSERT(index < struct_type->getNumElements());

      // get element offset
      offset_bits += code_context_.GetStructElementOffset(struct_type, index) * 8;

      // get inner type for next iteration
      type = struct_type->getElementType(index);
    } else {
      throw NotSupportedException(
          "unexpected type in extractvalue instruction");
    }
  }

  // assure that resulting type is correct
  PELOTON_ASSERT(type == extract_instruction->getType());

  // number if bits to shift is an immediate value!
  InsertBytecodeInstruction(
      extract_instruction, Opcode::extractvalue,
      {GetValueSlot(extract_instruction),
       GetValueSlot(extract_instruction->getAggregateOperand()),
       static_cast<index_t>(offset_bits)});
}

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton
