//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// bytecode_interpreter.h
//
// Identification: src/include/codegen/interpreter/bytecode_interpreter.h
//
// Copyright (c) 2015-2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/interpreter/bytecode_function.h"

#include "codegen/query.h"
#include "common/exception.h"
#include "common/overflow_builtins.h"

namespace peloton {
namespace codegen {
namespace interpreter {

/**
 * Holds the runtime information for a external funtion call. Because libffi
 * requires pointers to the actual value slots, this information is different
 * for every function activation, and can not be stored in the bytecode
 * function.
 */
struct CallActivation {
  ffi_cif call_interface;
  std::vector<value_t *> value_pointers;
  value_t *return_pointer;
};

class BytecodeInterpreter {
 public:
  /**
   * Executes a translated function with the interpreter
   * @param bytecode_function bytecode function that shall be executed
   * @param arguments vector of function arguments (stored as value_t). The
   * number of arguments must match the number expected by the executed
   * function.
   * @return return Value of the LLVM function or undefined if void.
   */
  static value_t ExecuteFunction(const BytecodeFunction &bytecode_function,
                                 const std::vector<value_t> &arguments);
  /**
   * Executes a translated function with the interpreter
   * (Wrapper for usage with a single char* argument)
   * @param bytecode_function  bytecode function that shall be executed, must
   * expect one argument.
   * @param arguments Char pointer argument of the function.
   */
  static void ExecuteFunction(const BytecodeFunction &bytecode_function,
                              char *param);

 private:
  explicit BytecodeInterpreter(const BytecodeFunction &bytecode_function);

  /**
   * Executes a function with the given arguments. The return value can
   * afterwards retrieved with GetReturnValue(). This function is also called
   * for internal function calls during execution.
   * @param arguments Vector of function arguments (stored as value_t). The
   * number of arguments must match the number expected by the executed
   * function.
   */
  void ExecuteFunction(const std::vector<value_t> &arguments);

  /**
   * Initializes the activation record by allocating the value slots, placing
   * function arguments and constants and preparing call contexts.
   * @param arguments Vector of function arguments (stored as value_t). The
   * number of arguments must match the number expected by the executed
   * function.
   */
  void InitializeActivationRecord(const std::vector<value_t> &arguments);

  /**
   * Returns the function return value _after_ execution.
   * @tparam type_t Expected return type.
   * @return Return value of executed function or undefined if void.
   */
  template <typename type_t>
  type_t GetReturnValue();

  /**
   * Get the current value of a value slot.
   * @tparam type_t requested type
   * @param index value slot index
   * @return value as requested type
   */
  template <typename type_t>
  ALWAYS_INLINE inline type_t GetValue(const index_t index) {
    PELOTON_ASSERT(index >= 0 && index < bytecode_function_.number_values_);
    return *reinterpret_cast<type_t *>(&values_[index]);
  }

  /**
   * Get the reference to a value slot. Usually SetValue() should be used
   * to set the values, but some use cases require pointers/references to the
   * slots.
   * @tparam type_t requested type
   * @param index value slot index
   * @return typed reference to the requested slot
   */
  template <typename type_t>
  ALWAYS_INLINE inline type_t &GetValueReference(const index_t index) {
    PELOTON_ASSERT(index >= 0 && index < bytecode_function_.number_values_);
    return reinterpret_cast<type_t &>(values_[index]);
  }

  /**
   * Set the current value of a slot
   * @tparam type_t requested type
   * @param index value slot index
   * @param value value of type type_t, that shall be set
   */
  template <typename type_t>
  ALWAYS_INLINE inline void SetValue(const index_t index, const type_t value) {
    PELOTON_ASSERT(index >= 0 && index < bytecode_function_.number_values_);
    *reinterpret_cast<type_t *>(&values_[index]) = value;

    DumpValue<type_t>(index);
  }

  /**
   * Advance the instruction pointer by a compile-time value.
   * @tparam number_instruction_slots size of current instruction
   * @param instruction current instruction pointer
   * @return new instruction pointer
   */
  template <size_t number_instruction_slots>
  ALWAYS_INLINE inline const Instruction *AdvanceIP(
      const Instruction *instruction) {
    auto next = reinterpret_cast<const Instruction *>(
        const_cast<instr_slot_t *>(
            reinterpret_cast<const instr_slot_t *>(instruction)) +
        number_instruction_slots);
    return next;
  }

  /**
   * Advance the instruction pointer by a run-time value.
   * @tparam number_instruction_slots size of current instruction
   * @param instruction current instruction pointer
   * @return new instruction pointer
   */
  ALWAYS_INLINE inline const Instruction *AdvanceIP(
      const Instruction *instruction, size_t number_instruction_slots) {
    auto next = reinterpret_cast<const Instruction *>(
        const_cast<instr_slot_t *>(
            reinterpret_cast<const instr_slot_t *>(instruction)) +
        number_instruction_slots);
    return next;
  }

  /**
   * Allocate memory and return a pointer to it. (Memory is managed and gets
   * freed after the interpreter exits)
   * @param number_bytes number of bytes to allocate
   * @return pointer to the allocated memory
   */
  uintptr_t AllocateMemory(size_t number_bytes);

/**
 * Dump the value of the given as value slot for debug purposes.
 * If LOG_TRACE is not enabled, this function compiles to a stub.
 * @param index value index of value slot to dump
 */
#ifdef LOG_TRACE_ENABLED
  template <typename type_t>
  void DumpValue(const index_t index) {
    std::ostringstream output;
    output << "  [" << std::dec << std::setw(3) << index
           << "] <= " << GetValue<type_t>(index) << "/0x" << std::hex
           << GetValue<type_t>(index);
    LOG_TRACE("%s", output.str().c_str());
  }
#else
  template <typename type_t>
  void DumpValue(UNUSED_ATTRIBUTE const index_t index) {}
#endif

  //--------------------------------------------------------------------------//
  //                          Instruction Handlers
  //
  // - The following functions are the instruction handlers for the bytecode
  //   instructions defined in bytecode_instructions.def .
  // - The signatures of those functions are not code style conform, as they are
  //   generated from the opcode mnemonic
  // - If the instruction is marked as a typed instruction in the .def file,
  //   it has a templated handler. Some handlers only support floating point or
  //   integer types, some both. Static asserts ensure this.
  // - Because all the handlers will get inlined in the dispatch area, their
  //   definition must be in this header file.
  //--------------------------------------------------------------------------//

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *addHandler(
      const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) +
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *subHandler(
      const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) -
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *mulHandler(
      const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) *
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *divHandler(
      const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) /
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sdivHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0],
                            (GetValue<type_signed_t>(instruction->args[1]) /
                             GetValue<type_signed_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uremHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) %
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *fremHandler(
      const Instruction *instruction) {
    static_assert(std::is_floating_point<type_t>::value,
                  "__func__ must only be used with floating point types");
    SetValue<type_t>(instruction->args[0],
                     (std::fmod(GetValue<type_t>(instruction->args[1]),
                                GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sremHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0],
                            (GetValue<type_signed_t>(instruction->args[1]) %
                             GetValue<type_signed_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *shlHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1])
                      << GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *lshrHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) >>
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *ashrHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<type_signed_t>(instruction->args[0],
                            (GetValue<type_signed_t>(instruction->args[1]) >>
                             GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *andHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) &
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *orHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) |
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *xorHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    SetValue<type_t>(instruction->args[0],
                     (GetValue<type_t>(instruction->args[1]) ^
                      GetValue<type_t>(instruction->args[2])));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *extractvalueHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (GetValue<value_t>(instruction->args[1]) >> instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *loadHandler(
      const Instruction *instruction) {
    SetValue<type_t>(instruction->args[0],
                     (*GetValue<type_t *>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *storeHandler(
      const Instruction *instruction) {
    *GetValue<type_t *>(instruction->args[0]) =
        GetValue<type_t>(instruction->args[1]);
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *alloca_arrayHandler(
      const Instruction *instruction) {
    size_t number_bytes =
        instruction->args[1] * GetValue<type_t>(instruction->args[2]);
    SetValue<uintptr_t>(instruction->args[0], (AllocateMemory(number_bytes)));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *allocaHandler(
      const Instruction *instruction) {
    size_t number_bytes = instruction->args[1];
    SetValue<uintptr_t>(instruction->args[0], (AllocateMemory(number_bytes)));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_eqHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) ==
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_neHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) !=
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_gtHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) >
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_ltHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) <
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_geHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) >=
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_leHandler(
      const Instruction *instruction) {
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_t>(instruction->args[1]) <=
                              GetValue<type_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sgtHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) >
                              GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sltHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) <
                              GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sgeHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) >=
                              GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *cmp_sleHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    SetValue<value_t>(
        instruction->args[0],
        (static_cast<value_t>(GetValue<type_signed_t>(instruction->args[1]) <=
                              GetValue<type_signed_t>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i8_i16Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i16>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i8_i32Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i32>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i8_i64Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i8>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i16_i32Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i16>::type;
    using dest_t = typename std::make_signed<i32>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i16_i64Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i16>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *sext_i32_i64Handler(
      const Instruction *instruction) {
    using src_t = typename std::make_signed<i32>::type;
    using dest_t = typename std::make_signed<i64>::type;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i16Handler(
      const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i16;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i32Handler(
      const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i32;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i8_i64Handler(
      const Instruction *instruction) {
    using src_t = i8;
    using dest_t = i64;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i16_i32Handler(
      const Instruction *instruction) {
    using src_t = i16;
    using dest_t = i32;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i16_i64Handler(
      const Instruction *instruction) {
    using src_t = i16;
    using dest_t = i64;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *zext_i32_i64Handler(
      const Instruction *instruction) {
    using src_t = i32;
    using dest_t = i64;
    SetValue<dest_t>(
        instruction->args[0],
        (static_cast<dest_t>(GetValue<src_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  // The FP<>Int casts are created in a two-level hierarchy
  // eg. the generated call to floattosiHandler<i8> is redirected to
  // tosiHandler<float, i8>

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *tosiHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<dest_type_t>::value,
                  "__func__ dest_type must be an integer type");
    static_assert(std::is_floating_point<src_type_t>::value,
                  "__func__ src_type must be a floating point type");
    using dest_type_signed_t = typename std::make_signed<dest_type_t>::type;

    SetValue<dest_type_signed_t>(
        instruction->args[0], (static_cast<dest_type_signed_t>(
                                  GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *touiHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<dest_type_t>::value,
                  "__func__ dest_type must be an integer type");
    static_assert(std::is_floating_point<src_type_t>::value,
                  "__func__ src_type must be a floating point type");

    SetValue<dest_type_t>(
        instruction->args[0],
        (static_cast<dest_type_t>(GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *sitoHandler(
      const Instruction *instruction) {
    static_assert(std::is_floating_point<dest_type_t>::value,
                  "__func__ dest_type must be a floating point type");
    static_assert(std::is_integral<src_type_t>::value,
                  "__func__ src_type must be an integer type");
    using src_type_signed_t = typename std::make_signed<src_type_t>::type;

    SetValue<dest_type_t>(instruction->args[0],
                          (static_cast<dest_type_t>(GetValue<src_type_signed_t>(
                              instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename src_type_t, typename dest_type_t>
  ALWAYS_INLINE inline const Instruction *uitoHandler(
      const Instruction *instruction) {
    static_assert(std::is_floating_point<dest_type_t>::value,
                  "__func__ dest_type must be a floating point type");
    static_assert(std::is_integral<src_type_t>::value,
                  "__func__ src_type must be an integer type");

    SetValue<dest_type_t>(
        instruction->args[0],
        (static_cast<dest_type_t>(GetValue<src_type_t>(instruction->args[1]))));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *floattosiHandler(
      const Instruction *instruction) {
    return tosiHandler<float, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *floattouiHandler(
      const Instruction *instruction) {
    return touiHandler<float, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sitofloatHandler(
      const Instruction *instruction) {
    return sitoHandler<type_t, float>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uitofloatHandler(
      const Instruction *instruction) {
    return uitoHandler<type_t, float>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *doubletosiHandler(
      const Instruction *instruction) {
    return tosiHandler<double, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *doubletouiHandler(
      const Instruction *instruction) {
    return touiHandler<double, type_t>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *sitodoubleHandler(
      const Instruction *instruction) {
    return sitoHandler<type_t, double>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *uitodoubleHandler(
      const Instruction *instruction) {
    return uitoHandler<type_t, double>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *gep_offsetHandler(
      const Instruction *instruction) {
    uintptr_t sum = GetValue<uintptr_t>(instruction->args[1]) +
                    static_cast<uintptr_t>(instruction->args[2]);
    SetValue<uintptr_t>(instruction->args[0], (sum));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *gep_arrayHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    uintptr_t product =
        GetValue<type_t>(instruction->args[1]) * instruction->args[2];
    SetValue<uintptr_t>(instruction->args[0],
                        (GetValue<uintptr_t>(instruction->args[0]) + product));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *phi_movHandler(
      const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0],
                      (GetValue<value_t>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *selectHandler(
      const Instruction *instruction) {
    value_t result;
    if (GetValue<i8>(instruction->args[1]) > 0)
      result = GetValue<value_t>(instruction->args[2]);
    else
      result = GetValue<value_t>(instruction->args[3]);

    SetValue<value_t>(instruction->args[0], (result));
    return AdvanceIP<2>(instruction);  // bigger slot size!
  }

  ALWAYS_INLINE inline const Instruction *call_externalHandler(
      const Instruction *instruction) {
    const ExternalCallInstruction *call_instruction =
        reinterpret_cast<const ExternalCallInstruction *>(instruction);
    CallActivation &call_activation =
        call_activations_[call_instruction->external_call_context];

    // call external function
    ffi_call(&call_activation.call_interface, call_instruction->function,
             call_activation.return_pointer,
             reinterpret_cast<void **>(call_activation.value_pointers.data()));

    if (bytecode_function_
            .external_call_contexts_[call_instruction->external_call_context]
            .dest_type != &ffi_type_void) {
      DumpValue<value_t>(
          bytecode_function_
              .external_call_contexts_[call_instruction->external_call_context]
              .dest_slot);
    }

    return AdvanceIP<2>(instruction);  // bigger slot size!
  }

  ALWAYS_INLINE inline const Instruction *call_internalHandler(
      const Instruction *instruction) {
    const InternalCallInstruction *call_instruction =
        reinterpret_cast<const InternalCallInstruction *>(instruction);

    std::vector<value_t> arguments(call_instruction->number_args);
    for (size_t i = 0; i < call_instruction->number_args; i++) {
      arguments[i] = GetValue<value_t>(call_instruction->args[i]);
    }

    value_t result = ExecuteFunction(
        bytecode_function_.sub_functions_[call_instruction->sub_function],
        arguments);
    SetValue(call_instruction->dest_slot, result);

    return AdvanceIP(
        instruction,
        bytecode_function_.GetInteralCallInstructionSlotSize(call_instruction));
  }

  ALWAYS_INLINE inline const Instruction *nop_movHandler(
      const Instruction *instruction) {
    SetValue<value_t>(instruction->args[0],
                      (GetValue<value_t>(instruction->args[1])));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *branch_uncondHandler(
      const Instruction *instruction) {
    return bytecode_function_.GetIPFromIndex(instruction->args[0]);
  }

  ALWAYS_INLINE inline const Instruction *branch_condHandler(
      const Instruction *instruction) {
    index_t next_bb;
    if (GetValue<i8>(instruction->args[0]) > 0)
      next_bb = instruction->args[2];
    else
      next_bb = instruction->args[1];

    return bytecode_function_.GetIPFromIndex(next_bb);
  }

  ALWAYS_INLINE inline const Instruction *branch_cond_ftHandler(
      const Instruction *instruction) {
    const Instruction *ip;
    if ((GetValue<value_t>(instruction->args[0]) & 0x1) > 0)
      ip = bytecode_function_.GetIPFromIndex(instruction->args[1]);
    else
      ip = AdvanceIP<1>(instruction);

    return ip;
  }

  ALWAYS_INLINE inline const Instruction *llvm_memcpyHandler(
      const Instruction *instruction) {
    PELOTON_MEMCPY(GetValue<void *>(instruction->args[0]),
              GetValue<void *>(instruction->args[1]),
              GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_memmoveHandler(
      const Instruction *instruction) {
    std::memmove(GetValue<void *>(instruction->args[0]),
                 GetValue<void *>(instruction->args[1]),
                 GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_memsetHandler(
      const Instruction *instruction) {
    PELOTON_MEMSET(GetValue<void *>(instruction->args[0]),
              GetValue<i8>(instruction->args[1]),
              GetValue<i64>(instruction->args[2]));
    return AdvanceIP<1>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_uadd_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    bool overflow = __builtin_add_overflow(
        GetValue<type_t>(instruction->args[2]),
        GetValue<type_t>(instruction->args[3]),
        &GetValueReference<type_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_sadd_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_add_overflow(
        GetValue<type_signed_t>(instruction->args[2]),
        GetValue<type_signed_t>(instruction->args[3]),
        &GetValueReference<type_signed_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_usub_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    bool overflow = __builtin_sub_overflow(
        GetValue<type_t>(instruction->args[2]),
        GetValue<type_t>(instruction->args[3]),
        &GetValueReference<type_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_ssub_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_sub_overflow(
        GetValue<type_signed_t>(instruction->args[2]),
        GetValue<type_signed_t>(instruction->args[3]),
        &GetValueReference<type_signed_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_umul_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    bool overflow = __builtin_mul_overflow(
        GetValue<type_t>(instruction->args[2]),
        GetValue<type_t>(instruction->args[3]),
        &GetValueReference<type_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  template <typename type_t>
  ALWAYS_INLINE inline const Instruction *llvm_smul_overflowHandler(
      const Instruction *instruction) {
    static_assert(std::is_integral<type_t>::value,
                  "__func__ must only be used with integer types");
    using type_signed_t = typename std::make_signed<type_t>::type;
    bool overflow = __builtin_mul_overflow(
        GetValue<type_signed_t>(instruction->args[2]),
        GetValue<type_signed_t>(instruction->args[3]),
        &GetValueReference<type_signed_t>(instruction->args[0]));

    DumpValue<type_t>(instruction->args[0]);

    SetValue<value_t>(instruction->args[1], (static_cast<value_t>(overflow)));
    return AdvanceIP<2>(instruction);
  }

  ALWAYS_INLINE inline const Instruction *llvm_sse42_crc32Handler(
      const Instruction *instruction) {
    SetValue<i64>(
        instruction->args[0],
        (__builtin_ia32_crc32di(GetValue<i64>(instruction->args[1]),
                                GetValue<i64>(instruction->args[2]))));
    return AdvanceIP<1>(instruction);
  }

  //--------------------------------------------------------------------------//

 private:
  /**
   * This static array holds the goto-pointer for the dispatch area for each
   * Opcode. It will be filled once, when the interpreter is called the
   * first time.
   */
  static void *label_pointers_[BytecodeFunction::GetNumberOpcodes()];

  /**
   * Value slots (register) for the current function activation.
   * (Aligned by something that is most likely the cache line size)
   */
  alignas(64) std::vector<value_t> values_;

  /**
   * Holds all allocations made with alloca. We do not need to access them,
   * but the unique pointer ensures they will be released at the end.
   */
  std::vector<std::unique_ptr<char[]>> allocations_;

  /**
   * Holds the call activation records for all external call instructions.
   * (Created during initialization)
   */
  std::vector<CallActivation> call_activations_;

  /**
   * Bytecode function used for execution.
   */
  const BytecodeFunction &bytecode_function_;

 private:
  // This class cannot be copy or move-constructed
  DISALLOW_COPY_AND_MOVE(BytecodeInterpreter);
};

}  // namespace interpreter
}  // namespace codegen
}  // namespace peloton